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

import java.util.concurrent.locks.ReentrantLock

import kafka.cluster.BrokerEndPoint
import kafka.consumer.PartitionTopicInfo
import kafka.message.{MessageAndOffset, ByteBufferMessageSet}
import kafka.utils.{Pool, ShutdownableThread, DelayedItem}
import kafka.common.{KafkaException, ClientIdAndBroker, TopicAndPartition}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.protocol.Errors
import AbstractFetcherThread._
import scala.collection.{mutable, Set, Map}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.yammer.metrics.core.Gauge

/**
 *  Abstract class for fetching data from multiple partitions from the same broker.
 *  用于从同一代理中的多个分区获取数据的抽象类
 */
abstract class AbstractFetcherThread(name: String,
                                     clientId: String,
                                     sourceBroker: BrokerEndPoint,
                                     fetchBackOffMs: Int = 0,
                                     isInterruptible: Boolean = true)
  extends ShutdownableThread(name, isInterruptible) {
  //拉取请求
  type REQ <: FetchRequest
  //拉取的数据
  type PD <: PartitionData
  //为每个主题分区设置拉取偏移量
  private val partitionMap = new mutable.HashMap[TopicAndPartition, PartitionFetchState] // a (topic, partition) -> partitionFetchState map
  private val partitionMapLock = new ReentrantLock
  private val partitionMapCond = partitionMapLock.newCondition()

  private val metricId = new ClientIdAndBroker(clientId, sourceBroker.host, sourceBroker.port)
  val fetcherStats = new FetcherStats(metricId)
  val fetcherLagStats = new FetcherLagStats(metricId)

  /* callbacks to be defined in subclass */

  // process fetched data
  def processPartitionData(topicAndPartition: TopicAndPartition, fetchOffset: Long, partitionData: PD)

  // handle a partition whose offset is out of range and return a new fetch offset
  def handleOffsetOutOfRange(topicAndPartition: TopicAndPartition): Long

  // deal with partitions with errors, potentially due to leadership changes
  def handlePartitionsWithErrors(partitions: Iterable[TopicAndPartition])

  protected def buildFetchRequest(partitionMap: Map[TopicAndPartition, PartitionFetchState]): REQ

  protected def fetch(fetchRequest: REQ): Map[TopicAndPartition, PD]

  override def shutdown(){
    initiateShutdown()
    inLock(partitionMapLock) {
      partitionMapCond.signalAll()
    }
    awaitShutdown()

    // we don't need the lock since the thread has finished shutdown and metric removal is safe
    fetcherStats.unregister()
    fetcherLagStats.unregister()
  }

  override def doWork() {
    //构建拉取请求
    val fetchRequest = inLock(partitionMapLock) {
      val fetchRequest = buildFetchRequest(partitionMap)
      if (fetchRequest.isEmpty) {
        trace("There are no active partitions. Back off for %d ms before sending a fetch request".format(fetchBackOffMs))
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
      }
      fetchRequest
    }

    if (!fetchRequest.isEmpty)
      processFetchRequest(fetchRequest)
  }

  /**
   * 处理拉取请求
   * @param fetchRequest 拉取请求
   */
  private def processFetchRequest(fetchRequest: REQ) {
    val partitionsWithError = new mutable.HashSet[TopicAndPartition]
    var responseData: Map[TopicAndPartition, PD] = Map.empty

    try {
      trace("Issuing to broker %d of fetch request %s".format(sourceBroker.id, fetchRequest))
      //子类调用拉取请求
      responseData = fetch(fetchRequest)
    } catch {
      case t: Throwable =>
        if (isRunning.get) {
          warn(s"Error in fetch $fetchRequest", t)
          inLock(partitionMapLock) {
            partitionsWithError ++= partitionMap.keys
            // there is an error occurred while fetching partitions, sleep a while
            partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
          }
        }
    }
    fetcherStats.requestRate.mark()

    if (responseData.nonEmpty) {
      // process fetched data 处理拉取的数据
      inLock(partitionMapLock) {

        responseData.foreach { case (topicAndPartition, partitionData) =>
          val TopicAndPartition(topic, partitionId) = topicAndPartition
          partitionMap.get(topicAndPartition).foreach(currentPartitionFetchState =>
            // we append to the log if the current offset is defined and it is the same as the offset requested during fetch 如果定义了当前偏移量，并且该偏移量与获取期间请求的偏移量相同，则将其追加到日志中
            if (fetchRequest.offset(topicAndPartition) == currentPartitionFetchState.offset) {
              Errors.forCode(partitionData.errorCode) match {
                case Errors.NONE =>
                  try {
                    //拉取回来的消息集
                    val messages = partitionData.toByteBufferMessageSet
                    val validBytes = messages.validBytes
                    val newOffset = messages.shallowIterator.toSeq.lastOption match {
                      case Some(m: MessageAndOffset) => m.nextOffset
                      case None => currentPartitionFetchState.offset
                    }
                    partitionMap.put(topicAndPartition, new PartitionFetchState(newOffset))
                    fetcherLagStats.getAndMaybePut(topic, partitionId).lag = Math.max(0L, partitionData.highWatermark - newOffset)
                    fetcherStats.byteRate.mark(validBytes)
                    // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread
                    processPartitionData(topicAndPartition, currentPartitionFetchState.offset, partitionData)
                  } catch {
                    //损坏的记录异常
                    case ime: CorruptRecordException =>
                      // we log the error and continue. This ensures two things 我们记录错误并继续。这确保了两件事
                      // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread down and cause other topic partition to also lag
                      //    如果主题分区中有损坏的消息，它不会使获取程序线程停止，并导致其他主题分区也延迟
                      // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes can cause this), we simply continue and
                      // should get fixed in the subsequent fetches
                      //    如果消息由于日志中的瞬态而损坏（截断、部分写入可能导致这种情况），我们只需继续，并应在后续的回迁中修复
                      logger.error("Found invalid messages during fetch for partition [" + topic + "," + partitionId + "] offset " + currentPartitionFetchState.offset  + " error " + ime.getMessage)
                    case e: Throwable =>
                      throw new KafkaException("error processing data for partition [%s,%d] offset %d"
                        .format(topic, partitionId, currentPartitionFetchState.offset), e)
                  }
                //偏移量超出范围
                case Errors.OFFSET_OUT_OF_RANGE =>
                  try {
                    //处理偏移量越界
                    val newOffset = handleOffsetOutOfRange(topicAndPartition)
                    partitionMap.put(topicAndPartition, new PartitionFetchState(newOffset))
                    error("Current offset %d for partition [%s,%d] out of range; reset offset to %d"
                      .format(currentPartitionFetchState.offset, topic, partitionId, newOffset))
                  } catch {
                    case e: Throwable =>
                      error("Error getting offset for partition [%s,%d] to broker %d".format(topic, partitionId, sourceBroker.id), e)
                      partitionsWithError += topicAndPartition
                  }
                case _ =>
                  if (isRunning.get) {
                    error("Error for partition [%s,%d] to broker %d:%s".format(topic, partitionId, sourceBroker.id,
                      partitionData.exception.get))
                    partitionsWithError += topicAndPartition
                  }
              }
            })
        }
      }
    }

    if (partitionsWithError.nonEmpty) {
      debug("handling partitions with error for %s".format(partitionsWithError))
      handlePartitionsWithErrors(partitionsWithError)
    }
  }

  def addPartitions(partitionAndOffsets: Map[TopicAndPartition, Long]) {
    partitionMapLock.lockInterruptibly()
    try {
      for ((topicAndPartition, offset) <- partitionAndOffsets) {
        // If the partitionMap already has the topic/partition, then do not update the map with the old offset 如果partitionMap已经具有topicpartition，则不要使用旧偏移量更新
        if (!partitionMap.contains(topicAndPartition))
          partitionMap.put(
            topicAndPartition,
            if (PartitionTopicInfo.isOffsetInvalid(offset)) new PartitionFetchState(handleOffsetOutOfRange(topicAndPartition))
            else new PartitionFetchState(offset)
          )}
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  /**
   * 延迟分区抓取
   * @param partitions
   * @param delay
   */
  def delayPartitions(partitions: Iterable[TopicAndPartition], delay: Long) {
    partitionMapLock.lockInterruptibly()
    try {
      for (partition <- partitions) {
        partitionMap.get(partition).foreach (currentPartitionFetchState =>
          if (currentPartitionFetchState.isActive)
            partitionMap.put(partition, new PartitionFetchState(currentPartitionFetchState.offset, new DelayedItem(delay)))
        )
      }
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  def removePartitions(topicAndPartitions: Set[TopicAndPartition]) {
    partitionMapLock.lockInterruptibly()
    try {
      topicAndPartitions.foreach { topicAndPartition =>
        partitionMap.remove(topicAndPartition)
        fetcherLagStats.unregister(topicAndPartition.topic, topicAndPartition.partition)
      }
    } finally partitionMapLock.unlock()
  }

  def partitionCount() = {
    partitionMapLock.lockInterruptibly()
    try partitionMap.size
    finally partitionMapLock.unlock()
  }

}

object AbstractFetcherThread {

  trait FetchRequest {
    def isEmpty: Boolean
    def offset(topicAndPartition: TopicAndPartition): Long
  }

  trait PartitionData {
    def errorCode: Short
    def exception: Option[Throwable]
    def toByteBufferMessageSet: ByteBufferMessageSet
    def highWatermark: Long
  }

}

object FetcherMetrics {
  val ConsumerLag = "ConsumerLag"
  val RequestsPerSec = "RequestsPerSec"
  val BytesPerSec = "BytesPerSec"
}

class FetcherLagMetrics(metricId: ClientIdTopicPartition) extends KafkaMetricsGroup {

  private[this] val lagVal = new AtomicLong(-1L)
  private[this] val tags = Map(
    "clientId" -> metricId.clientId,
    "topic" -> metricId.topic,
    "partition" -> metricId.partitionId.toString)

  newGauge(FetcherMetrics.ConsumerLag,
    new Gauge[Long] {
      def value = lagVal.get
    },
    tags
  )

  def lag_=(newLag: Long) {
    lagVal.set(newLag)
  }

  def lag = lagVal.get

  def unregister() {
    removeMetric(FetcherMetrics.ConsumerLag, tags)
  }
}

class FetcherLagStats(metricId: ClientIdAndBroker) {
  private val valueFactory = (k: ClientIdTopicPartition) => new FetcherLagMetrics(k)
  val stats = new Pool[ClientIdTopicPartition, FetcherLagMetrics](Some(valueFactory))

  def getAndMaybePut(topic: String, partitionId: Int): FetcherLagMetrics = {
    stats.getAndMaybePut(new ClientIdTopicPartition(metricId.clientId, topic, partitionId))
  }

  def unregister(topic: String, partitionId: Int) {
    val lagMetrics = stats.remove(new ClientIdTopicPartition(metricId.clientId, topic, partitionId))
    if (lagMetrics != null) lagMetrics.unregister()
  }

  def unregister() {
    stats.keys.toBuffer.foreach { key: ClientIdTopicPartition =>
      unregister(key.topic, key.partitionId)
    }
  }
}

class FetcherStats(metricId: ClientIdAndBroker) extends KafkaMetricsGroup {
  val tags = Map("clientId" -> metricId.clientId,
    "brokerHost" -> metricId.brokerHost,
    "brokerPort" -> metricId.brokerPort.toString)

  val requestRate = newMeter(FetcherMetrics.RequestsPerSec, "requests", TimeUnit.SECONDS, tags)

  val byteRate = newMeter(FetcherMetrics.BytesPerSec, "bytes", TimeUnit.SECONDS, tags)

  def unregister() {
    removeMetric(FetcherMetrics.RequestsPerSec, tags)
    removeMetric(FetcherMetrics.BytesPerSec, tags)
  }

}

case class ClientIdTopicPartition(clientId: String, topic: String, partitionId: Int) {
  override def toString = "%s-%s-%d".format(clientId, topic, partitionId)
}

/**
  * case class to keep partition offset and its state(active , inactive)
  * 保持分区偏移量及其状态（活动、非活动）的case类
  */
case class PartitionFetchState(offset: Long, delay: DelayedItem) {

  def this(offset: Long) = this(offset, new DelayedItem(0))

  def isActive: Boolean = { delay.getDelay(TimeUnit.MILLISECONDS) == 0 }

  override def toString = "%d-%b".format(offset, isActive)
}
