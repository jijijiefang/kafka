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

import java.util.concurrent.TimeUnit

import kafka.api.FetchResponsePartitionData
import kafka.api.PartitionFetchInfo
import kafka.common.TopicAndPartition
import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.common.errors.{NotLeaderForPartitionException, UnknownTopicOrPartitionException}

import scala.collection._

/**
 * 拉取分区状态
 * @param startOffsetMetadata 起始偏移元数据
 * @param fetchInfo 分区拉取信息
 */
case class FetchPartitionStatus(startOffsetMetadata: LogOffsetMetadata, fetchInfo: PartitionFetchInfo) {

  override def toString = "[startOffsetMetadata: " + startOffsetMetadata + ", " +
                          "fetchInfo: " + fetchInfo + "]"
}

/**
 * The fetch metadata maintained by the delayed fetch operation
 * 由延迟的获取操作维护的获取元数据
 */
case class FetchMetadata(fetchMinBytes: Int,
                         fetchOnlyLeader: Boolean,
                         fetchOnlyCommitted: Boolean,
                         isFromFollower: Boolean,
                         fetchPartitionStatus: Map[TopicAndPartition, FetchPartitionStatus]) {

  override def toString = "[minBytes: " + fetchMinBytes + ", " +
                          "onlyLeader:" + fetchOnlyLeader + ", "
                          "onlyCommitted: " + fetchOnlyCommitted + ", "
                          "partitionStatus: " + fetchPartitionStatus + "]"
}
/**
 * A delayed fetch operation that can be created by the replica manager and watched
 * in the fetch operation purgatory
 * 延迟抓取操作，由副本管理器创建且由延时操作管理器监视
 */
class DelayedFetch(delayMs: Long,
                   fetchMetadata: FetchMetadata,
                   replicaManager: ReplicaManager,
                   responseCallback: Map[TopicAndPartition, FetchResponsePartitionData] => Unit)
  extends DelayedOperation(delayMs) {

  /**
   * The operation can be completed if:
   * 如果满足以下条件，则可以完成该操作：
   * Case A: This broker is no longer the leader for some partitions it tries to fetch 案例A：此代理不再是它尝试获取的某些分区Leader
   * Case B: This broker does not know of some partitions it tries to fetch 案例B：此代理不知道它尝试获取的某些分区
   * Case C: The fetch offset locates not on the last segment of the log 案例C：获取偏移量不在日志的最后一段
   * Case D: The accumulated bytes from all the fetching partitions exceeds the minimum bytes 案例D：所有获取分区的累积字节数超过了最小字节数
   *
   * Upon completion, should return whatever data is available for each valid partition
   * 完成后，应该返回每个有效分区可用的任何数据
   */
  override def tryComplete() : Boolean = {
    var accumulatedSize = 0
    fetchMetadata.fetchPartitionStatus.foreach {
      case (topicAndPartition, fetchStatus) =>
        val fetchOffset = fetchStatus.startOffsetMetadata
        try {
          if (fetchOffset != LogOffsetMetadata.UnknownOffsetMetadata) {
            //获取此主题分区的本地Leader副本
            val replica = replicaManager.getLeaderReplicaIfLocal(topicAndPartition.topic, topicAndPartition.partition)
            val endOffset =
              if (fetchMetadata.fetchOnlyCommitted)
                replica.highWatermark
              else
                replica.logEndOffset

            // Go directly to the check for Case D if the message offsets are the same. If the log segment 如果消息偏移量相同，则直接转到检查案例D。
            // has just rolled, then the high watermark offset will remain the same but be on the old segment,
            // which would incorrectly be seen as an instance of Case C. 如果日志段刚刚滚动，则高水位线偏移量将保持不变，但位于旧段上，这将被错误地视为案例C的一个实例
            if (endOffset.messageOffset != fetchOffset.messageOffset) {
              if (endOffset.onOlderSegment(fetchOffset)) {
                // Case C, this can happen when the new fetch operation is on a truncated leader 在案例C中，当新的获取操作位于截断的Leader上时，可能会发生这种情况
                debug("Satisfying fetch %s since it is fetching later segments of partition %s.".format(fetchMetadata, topicAndPartition))
                return forceComplete()
              } else if (fetchOffset.onOlderSegment(endOffset)) {
                // Case C, this can happen when the fetch operation is falling behind the current segment
                // or the partition has just rolled a new segment 在案例C中，当fetch操作落后于当前段或者分区刚刚滚动了一个新段时，就会发生这种情况
                debug("Satisfying fetch %s immediately since it is fetching older segments.".format(fetchMetadata))
                return forceComplete()
              } else if (fetchOffset.messageOffset < endOffset.messageOffset) {
                // we need take the partition fetch size as upper bound when accumulating the bytes 在累积字节时，我们需要将分区获取大小作为上限
                accumulatedSize += math.min(endOffset.positionDiff(fetchOffset), fetchStatus.fetchInfo.fetchSize)
              }
            }
          }
        } catch {
          case utpe: UnknownTopicOrPartitionException => // Case B
            debug("Broker no longer know of %s, satisfy %s immediately".format(topicAndPartition, fetchMetadata))
            return forceComplete()
          case nle: NotLeaderForPartitionException =>  // Case A
            debug("Broker is no longer the leader of %s, satisfy %s immediately".format(topicAndPartition, fetchMetadata))
            return forceComplete()
        }
    }

    // Case D 积攒的字节数达到拉取最小字节数
    if (accumulatedSize >= fetchMetadata.fetchMinBytes)
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    if (fetchMetadata.isFromFollower)
      DelayedFetchMetrics.followerExpiredRequestMeter.mark()
    else
      DelayedFetchMetrics.consumerExpiredRequestMeter.mark()
  }

  /**
   * Upon completion, read whatever data is available and pass to the complete callback
   * 操作完成回调
   */
  override def onComplete() {
    val logReadResults = replicaManager.readFromLocalLog(fetchMetadata.fetchOnlyLeader,
      fetchMetadata.fetchOnlyCommitted,
      fetchMetadata.fetchPartitionStatus.mapValues(status => status.fetchInfo))

    val fetchPartitionData = logReadResults.mapValues(result =>
      FetchResponsePartitionData(result.errorCode, result.hw, result.info.messageSet))

    responseCallback(fetchPartitionData)
  }
}

object DelayedFetchMetrics extends KafkaMetricsGroup {
  private val FetcherTypeKey = "fetcherType"
  val followerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "follower"))
  val consumerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "consumer"))
}

