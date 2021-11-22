/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.utils._
import kafka.utils.timer._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.metrics.KafkaMetricsGroup

import java.util.LinkedList
import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.kafka.common.utils.Utils

import scala.collection._

import com.yammer.metrics.core.Gauge


/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example
 * a delayed produce operation could be waiting for specified number of acks; or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 *
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true. onComplete() can be triggered by either
 * forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls
 * forceComplete().
 *
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
 */
abstract class DelayedOperation(override val delayMs: Long) extends TimerTask with Logging {

  private val completed = new AtomicBoolean(false)

  /*
   * Force completing the delayed operation, if not already completed.如果尚未完成，则强制完成延迟操作
   * This function can be triggered when 此功能可在以下情况下触发：
   *
   * 1. The operation has been verified to be completable inside tryComplete() 已验证操作在tryComplete()内部是可完成的
   * 2. The operation has expired and hence needs to be completed right now 该操作已过期，因此需要立即完成
   *
   * Return true iff the operation is completed by the caller: note that
   * concurrent threads can try to complete the same operation, but only
   * the first thread will succeed in completing the operation and return
   * true, others will still return false
   */
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) {
      // cancel the timeout timer
      cancel()
      onComplete()
      true
    } else {
      false
    }
  }

  /**
   * Check if the delayed operation is already completed 检查延迟的操作是否已经完成
   */
  def isCompleted(): Boolean = completed.get()

  /**
   * Call-back to execute when a delayed operation gets expired and hence forced to complete.
   * 当延迟的操作过期并因此被迫完成时，回调以执行
   */
  def onExpiration(): Unit

  /**
   * Process for completing an operation; This function needs to be defined
   * in subclasses and will be called exactly once in forceComplete()
   * 完成操作的过程；此函数需要在子类中定义，并且在forceComplete（）中只调用一次
   */
  def onComplete(): Unit

  /*
   * Try to complete the delayed operation by first checking if the operation
   * can be completed by now. If yes execute the completion logic by calling
   * forceComplete() and return true iff forceComplete returns true; otherwise return false
   * 首先检查操作是否可以在现在完成，尝试完成延迟的操作。
   * 如果是，则通过调用forceComplete执行完成逻辑并返回true，如果forceComplete返回true；否则返回false
   * This function needs to be defined in subclasses
   */
  def tryComplete(): Boolean

  /*
   * run() method defines a task that is executed on timeout run方法定义超时时执行的任务
   */
  override def run(): Unit = {
    if (forceComplete())
      onExpiration()
  }
}

/**
 * 延时操作管理器（延时操作炼狱）
 * Purgatory直译为“炼狱”，但丁的《神曲》中有炼狱的相关描述。
 * 炼狱共有9层，在生前犯有罪过但可以得到宽恕的灵魂，按照人类的七宗罪（傲慢、忌妒、愤怒、怠惰、贪财、贪食、贪色）分别在这里修炼洗涤，而后一层层升向光明和天堂。
 * Kafka 中采用这一称谓，将延时操作看作需要被洗涤的灵魂，在炼狱中慢慢修炼，等待解脱升入天堂（即完成延时操作）
 */
object DelayedOperationPurgatory {

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval)
  }

}

/**
 * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 * 延时操作管理器（延时操作炼狱）
 */
class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                       timeoutTimer: Timer,
                                                       brokerId: Int = 0,
                                                       purgeInterval: Int = 1000,
                                                       reaperEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {

  /* a list of operation watching keys 操作监视键列表*/
  private val watchersForKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

  private val removeWatchersLock = new ReentrantReadWriteLock()

  // the number of estimated total operations in the purgatory 炼狱中估计的总操作数
  private[this] val estimatedTotalOperations = new AtomicInteger(0)

  /* background thread expiring operations that have timed out 后台线程正在终止已超时的操作*/
  private val expirationReaper = new ExpiredOperationReaper()

  private val metricsTags = Map("delayedOperation" -> purgatoryName)

  newGauge(
    "PurgatorySize",
    new Gauge[Int] {
      def value = watched()
    },
    metricsTags
  )

  newGauge(
    "NumDelayedOperations",
    new Gauge[Int] {
      def value = delayed()
    },
    metricsTags
  )

  if (reaperEnabled)
    expirationReaper.start()

  /**
   * Check if the operation can be completed, if not watch it based on the given watch keys
   * 检查操作是否可以完成，如果不能，请根据给定的观察键进行观察
   * Note that a delayed operation can be watched on multiple keys. It is possible that
   * an operation is completed after it has been added to the watch list for some, but
   * not all of the keys. In this case, the operation is considered completed and won't
   * be added to the watch list of the remaining keys. The expiration reaper thread will
   * remove this operation from any watcher list in which the operation exists.
   *
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.size > 0, "The watch key list can't be empty")

    // The cost of tryComplete() is typically proportional to the number of keys. Calling
    // tryComplete() for each key is going to be expensive if there are many keys. Instead,
    // we do the check in the following way. Call tryComplete(). If the operation is not completed,
    // we just add the operation to all keys. Then we call tryComplete() again. At this time, if
    // the operation is still not completed, we are guaranteed that it won't miss any future triggering
    // event since the operation is already on the watcher list for all keys. This does mean that
    // if the operation is completed (by another thread) between the two tryComplete() calls, the
    // operation is unnecessarily added for watch. However, this is a less severe issue since the
    // expire reaper will clean it up periodically.
    //上来就尝试是否可以完成
    var isCompletedByMe = operation synchronized operation.tryComplete()
    //如果完成直接返回true
    if (isCompletedByMe)
      return true

    var watchCreated = false
    //遍历watchKeys
    for(key <- watchKeys) {
      // If the operation is already completed, stop adding it to the rest of the watcher list.如果延迟操作已完成，不用把它加入到监视队列
      if (operation.isCompleted())
        return false
      watchForOperation(key, operation)//延迟操作加入观察者观察队列

      if (!watchCreated) {
        watchCreated = true
        estimatedTotalOperations.incrementAndGet()
      }
    }
    //尝试完成延迟操作
    isCompletedByMe = operation synchronized operation.tryComplete()
    if (isCompletedByMe)
      return true

    // if it cannot be completed by now and hence is watched, add to the expire queue also 如果现在无法完成，因此被监视，则也将其添加到expire队列
    if (! operation.isCompleted()) {
      //加入时间轮
      //操作添加到时间轮里
      timeoutTimer.add(operation)
      //如果操作已完成，则取消
      if (operation.isCompleted()) {
        // cancel the timer task
        operation.cancel()
      }
    }

    false
  }

  /**
   * Check if some some delayed operations can be completed with the given watch key,
   * and if yes complete them.
   * 检查是否可以使用给定的手表键完成某些延迟操作，如果可以，则完成这些操作
   * @return the number of completed operations during this process
   */
  def checkAndComplete(key: Any): Int = {
    val watchers = inReadLock(removeWatchersLock) { watchersForKey.get(key) }
    //不存在返回0
    if(watchers == null)
      0
    else//存在，尝试完成
      watchers.tryCompleteWatched()
  }

  /**
   * Return the total size of watch lists the purgatory. Since an operation may be watched
   * on multiple lists, and some of its watched entries may still be in the watch lists
   * even when it has been completed, this number may be larger than the number of real operations watched
   * 返回炼狱表的总大小。由于一个操作可能会在多个列表上被监视，并且即使它已经完成，它的一些被监视条目可能仍然在监视列表中，因此该数字可能大于被监视的实际操作的数量返回炼狱里观察的总数
   */
  def watched() = allWatchers.map(_.watched).sum

  /**
   * Return the number of delayed operations in the expiry queue
   * 返回过期队列中的延迟操作数
   */
  def delayed() = timeoutTimer.size

  /*
   * Return all the current watcher lists,
   * note that the returned watchers may be removed from the list by other threads
   * 返回所有当前观察者列表，请注意，返回的观察者可能会被其他线程从列表中删除
   */
  private def allWatchers = inReadLock(removeWatchersLock) { watchersForKey.values }

  /*
   * Return the watch list of the given key, note that we need to
   * grab the removeWatchersLock to avoid the operation being added to a removed watcher list
   * 监视此延迟操作
   */
  private def watchForOperation(key: Any, operation: T) {
    inReadLock(removeWatchersLock) {
      //获取监视着
      val watcher = watchersForKey.getAndMaybePut(key)
      //加入监视者
      watcher.watch(operation)
    }
  }

  /*
   * Remove the key from watcher lists if its list is empty
   */
  private def removeKeyIfEmpty(key: Any, watchers: Watchers) {
    inWriteLock(removeWatchersLock) {
      // if the current key is no longer correlated to the watchers to remove, skip
      if (watchersForKey.get(key) != watchers)
        return

      if (watchers != null && watchers.watched == 0) {
        watchersForKey.remove(key)
      }
    }
  }

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown() {
    if (reaperEnabled)
      expirationReaper.shutdown()
    timeoutTimer.shutdown()
  }

  /**
   * A linked list of watched delayed operations based on some key
   * 基于某个键的监视的延迟操作的链接列表
   */
  private class Watchers(val key: Any) {
    //用于存放和这个key关联的所有操作,一个key可以关联多个操作, 同时一个操作也可以被多个key关联(即位于多个Watchers对象中)
    private[this] val operations = new LinkedList[T]()

    def watched: Int = operations synchronized operations.size

    // add the element to watch 添加元素到观察列表
    def watch(t: T) {
      operations synchronized operations.add(t)
    }

    // traverse the list and try to complete some watched elements
    /**
     * 遍历列表，尝试完成观察的元素
     * @return
     */
    def tryCompleteWatched(): Int = {

      var completed = 0
      operations synchronized {
        val iter = operations.iterator()
        while (iter.hasNext) {
          val curr = iter.next()
          //如果已完成则移除
          if (curr.isCompleted) {
            // another thread has completed this operation, just remove it
            iter.remove()
            //如果未完成，则尝试立即完成，如果可以完成，则移除
          } else if (curr synchronized curr.tryComplete()) {
            completed += 1
            iter.remove()
          }
        }
      }

      if (operations.size == 0)
        removeKeyIfEmpty(key, this)

      completed
    }

    // traverse the list and purge elements that are already completed by others
    /**
     * 删除链表中所有已经完成的操作
     * @return
     */
    def purgeCompleted(): Int = {
      var purged = 0
      operations synchronized {
        val iter = operations.iterator()
        while (iter.hasNext) {
          val curr = iter.next()
          if (curr.isCompleted) {
            iter.remove()
            purged += 1
          }
        }
      }

      if (operations.size == 0)
        removeKeyIfEmpty(key, this)

      purged
    }
  }

  /**
   * 时间推进
   * @param timeoutMs 超时时间
   */
  def advanceClock(timeoutMs: Long) {
    //系统定时器时间向前推进
    timeoutTimer.advanceClock(timeoutMs)

    // Trigger a purge if the number of completed but still being watched operations is larger than
    // the purge threshold. That number is computed by the difference btw the estimated total number of
    // operations and the number of pending delayed operations.
    if (estimatedTotalOperations.get - delayed > purgeInterval) {
      // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
      // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
      // a little overestimated total number of operations.
      estimatedTotalOperations.getAndSet(delayed)
      debug("Begin purging watch lists")
      val purged = allWatchers.map(_.purgeCompleted()).sum
      debug("Purged %d elements from watch lists.".format(purged))
    }
  }

  /**
   * A background reaper to expire delayed operations that have timed out
   * 后台收割机将使已超时的延迟操作过期
   */
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d".format(brokerId),
    false) {

    override def doWork() {
      advanceClock(200L)
    }
  }
}
