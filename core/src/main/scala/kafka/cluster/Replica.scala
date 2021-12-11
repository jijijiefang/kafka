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

package kafka.cluster

import kafka.log.Log
import kafka.utils.{SystemTime, Time, Logging}
import kafka.server.{LogReadResult, LogOffsetMetadata}
import kafka.common.KafkaException

import java.util.concurrent.atomic.AtomicLong

/**
 * 副本
 * @param brokerId
 * @param partition
 * @param time
 * @param initialHighWatermarkValue
 * @param log
 */
class Replica(val brokerId: Int,
              val partition: Partition,
              time: Time = SystemTime,
              initialHighWatermarkValue: Long = 0L,
              val log: Option[Log] = None) extends Logging {
  // the high watermark offset value, in non-leader replicas only its message offsets are kept 高水位偏移量值，在非Leader副本中，仅保留其消息偏移量
  @volatile private[this] var highWatermarkMetadata: LogOffsetMetadata = new LogOffsetMetadata(initialHighWatermarkValue)
  // the log end offset value, kept in all replicas; 日志结束偏移量值，保存在所有副本中
  // for local replica it is the log's end offset, for remote replicas its value is only updated by follower fetch 对于本地副本，它是日志的结束偏移量；对于远程副本，它的值仅由Follower获取更新
  @volatile private[this] var logEndOffsetMetadata: LogOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata

  val topic = partition.topic
  val partitionId = partition.partitionId

  def isLocal: Boolean = {
  }
  log match {
    case Some(l) => true
    case None => false
  }

  private[this] val lastCaughtUpTimeMsUnderlying = new AtomicLong(time.milliseconds)

  def lastCaughtUpTimeMs = lastCaughtUpTimeMsUnderlying.get()

  /**
   * 更新日志读取结果,更新副本LEO和lastCaughtUpTimeMs
   * @param logReadResult
   */
  def updateLogReadResult(logReadResult : LogReadResult) {
    //更新此副本的LEO
    logEndOffset = logReadResult.info.fetchOffsetMetadata

    /* If the request read up to the log end offset snapshot when the read was initiated, 如果在启动读取时请求读取到日志结束偏移量快照，
     * set the lastCaughtUpTimeMsUnderlying to the current time. 请将LastCaughtUptimesUnderling设置为当前时间。
     * This means that the replica is fully caught up. 这意味着复制副本已完全跟上。
     */
    if(logReadResult.isReadFromLogEnd) {
      //更新此副本的lastCaughtUpTimeMs
      lastCaughtUpTimeMsUnderlying.set(time.milliseconds)
    }
  }

  private def logEndOffset_=(newLogEndOffset: LogOffsetMetadata) {
    if (isLocal) {
      throw new KafkaException("Should not set log end offset on partition [%s,%d]'s local replica %d".format(topic, partitionId, brokerId))
    } else {
      logEndOffsetMetadata = newLogEndOffset
      trace("Setting log end offset for replica %d for partition [%s,%d] to [%s]"
        .format(brokerId, topic, partitionId, logEndOffsetMetadata))
    }
  }

  def logEndOffset =
    if (isLocal)
      log.get.logEndOffsetMetadata
    else
      logEndOffsetMetadata

  /**
   * 更新HW
   * @param newHighWatermark
   */
  def highWatermark_=(newHighWatermark: LogOffsetMetadata) {
    if (isLocal) {
      highWatermarkMetadata = newHighWatermark
      trace("Setting high watermark for replica %d partition [%s,%d] on broker %d to [%s]"
        .format(brokerId, topic, partitionId, brokerId, newHighWatermark))
    } else {
      throw new KafkaException("Should not set high watermark on partition [%s,%d]'s non-local replica %d".format(topic, partitionId, brokerId))
    }
  }

  def highWatermark = highWatermarkMetadata

  def convertHWToLocalOffsetMetadata() = {
    if (isLocal) {
      highWatermarkMetadata = log.get.convertToOffsetMetadata(highWatermarkMetadata.messageOffset)
    } else {
      throw new KafkaException("Should not construct complete high watermark on partition [%s,%d]'s non-local replica %d".format(topic, partitionId, brokerId))
    }
  }

  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[Replica]))
      return false
    val other = that.asInstanceOf[Replica]
    if(topic.equals(other.topic) && brokerId == other.brokerId && partition.equals(other.partition))
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17*brokerId + partition.hashCode()
  }


  override def toString(): String = {
    val replicaString = new StringBuilder
    replicaString.append("ReplicaId: " + brokerId)
    replicaString.append("; Topic: " + topic)
    replicaString.append("; Partition: " + partition.partitionId)
    replicaString.append("; isLocal: " + isLocal)
    if(isLocal) replicaString.append("; Highwatermark: " + highWatermark)
    replicaString.toString()
  }
}
