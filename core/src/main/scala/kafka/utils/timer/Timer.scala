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
package kafka.utils.timer

import java.util.concurrent.{DelayQueue, Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.Utils

trait Timer {
  /**
    * Add a new task to this executor. It will be executed after the task's delay
    * (beginning from the time of submission)
    * 将新任务添加到此执行器。它将在任务延迟后执行（从提交时开始
    * @param timerTask the task to add
    */
  def add(timerTask: TimerTask): Unit

  /**
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
    * 提前内部时钟，执行在经过的超时持续时间内到期的任何任务
    * @param timeoutMs
    * @return whether or not any tasks were executed
    */
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * Get the number of tasks pending execution
    * 获取挂起执行的任务数
    * @return the number of tasks
    */
  def size: Int

  /**
    * Shutdown the timer service, leaving pending tasks unexecuted
    * 关闭计时器服务，保留未执行的挂起任务
    */
  def shutdown(): Unit
}

@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1,
                  wheelSize: Int = 20,
                  startMs: Long = System.currentTimeMillis) extends Timer {

  // timeout timer
  private[this] val taskExecutor = Executors.newFixedThreadPool(1, new ThreadFactory() {
    def newThread(runnable: Runnable): Thread =
      Utils.newThread("executor-"+executorName, runnable, false)
  })

  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  private[this] val taskCounter = new AtomicInteger(0)
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  // Locks used to protect data structures while ticking
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  def add(timerTask: TimerTask): Unit = {
    readLock.lock()
    try {
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + System.currentTimeMillis()))
    } finally {
      readLock.unlock()
    }
  }

  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    if (!timingWheel.add(timerTaskEntry)) {
      // Already expired or cancelled
      if (!timerTaskEntry.cancelled)
        taskExecutor.submit(timerTaskEntry.timerTask)
    }
  }

  private[this] val reinsert = (timerTaskEntry: TimerTaskEntry) => addTimerTaskEntry(timerTaskEntry)

  /*
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   * 如果存在过期的存储桶，则提前时钟。如果调用时没有任何过期的bucket，则在放弃之前等待timeoutMs
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) {
      writeLock.lock()
      try {
        while (bucket != null) {
          //时间轮指针向前推动
          timingWheel.advanceClock(bucket.getExpiration())
          bucket.flush(reinsert)
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }

  def size: Int = taskCounter.get

  override def shutdown() {
    taskExecutor.shutdown()
  }

}

