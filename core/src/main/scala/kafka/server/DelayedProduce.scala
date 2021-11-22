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

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Pool
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import scala.collection._

/**
 * 消息生产分区状态
 * @param requiredOffset
 * @param responseStatus
 */
case class ProducePartitionStatus(requiredOffset: Long, responseStatus: PartitionResponse) {
  @volatile var acksPending = false

  override def toString = "[acksPending: %b, error: %d, startOffset: %d, requiredOffset: %d]"
    .format(acksPending, responseStatus.errorCode, responseStatus.baseOffset, requiredOffset)
}

/**
 * The produce metadata maintained by the delayed produce operation
 * 由延迟的生产操作维护的生产元数据
 */
case class ProduceMetadata(produceRequiredAcks: Short,
                           produceStatus: Map[TopicPartition, ProducePartitionStatus]) {

  override def toString = "[requiredAcks: %d, partitionStatus: %s]"
    .format(produceRequiredAcks, produceStatus)
}

/**
 * A delayed produce operation that can be created by the replica manager and watched
 * in the produce operation purgatory
 * 一种延迟的生产操作，可由副本管理器创建并在生产操作炼狱中监视
 */
class DelayedProduce(delayMs: Long,
                     produceMetadata: ProduceMetadata,
                     replicaManager: ReplicaManager,
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit)
  extends DelayedOperation(delayMs) {

  // first update the acks pending variable according to the error code 首先根据错误代码更新acksPending
  produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
    if (status.responseStatus.errorCode == Errors.NONE.code) {
      // Timeout error state will be cleared when required acks are received 收到所需的确认后，超时错误状态将被清除
      status.acksPending = true
      status.responseStatus.errorCode = Errors.REQUEST_TIMED_OUT.code
    } else {
      status.acksPending = false
    }

    trace("Initial partition status for %s is %s".format(topicPartition, status))
  }

  /**
   * The delayed produce operation can be completed if every partition
   * it produces to is satisfied by one of the following:
   * 如果延迟生成的每个分区满足以下条件之一，则可以完成延迟生成操作：
   * Case A: This broker is no longer the leader: set an error in response 当前Broker已经不是Leader，设置错误
   * Case B: This broker is the leader: 当前Broker是Leader
   *   B.1 - If there was a local error thrown while checking if at least requiredAcks  如果在检查是否至少requiredAcks副本已完成此操作时引发了本地错误：请在响应中设置错误
   *         replicas have caught up to this operation: set an error in response
   *   B.2 - Otherwise, set the response with no error. 否则，将响应设置为无错误
   */
  override def tryComplete(): Boolean = {
    // check for each partition if it still has pending acks 检查每个分区是否仍有挂起的acks
    produceMetadata.produceStatus.foreach { case (topicAndPartition, status) =>
      trace("Checking produce satisfaction for %s, current status %s"
        .format(topicAndPartition, status))
      // skip those partitions that have already been satisfied 跳过已经满足的分区
      if (status.acksPending) {
        val partitionOpt = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition)
        //hasEnough是否满足条件
        val (hasEnough, errorCode) = partitionOpt match {
          case Some(partition) =>
            //检查是否有足够副本达到需要的偏移量
            partition.checkEnoughReplicasReachOffset(status.requiredOffset)
          case None =>
            // Case A
            (false, Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
        }
        //Leader副本本地错误
        if (errorCode != Errors.NONE.code) {
          // Case B.1
          status.acksPending = false
          status.responseStatus.errorCode = errorCode
        //满足条件，无错误
        } else if (hasEnough) {
          // Case B.2
          status.acksPending = false
          status.responseStatus.errorCode = Errors.NONE.code
        }
      }
    }

    // check if each partition has satisfied at lease one of case A and case B 检查每个分区是否至少满足案例A和案例B中的一个
    if (! produceMetadata.produceStatus.values.exists(p => p.acksPending))
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      if (status.acksPending) {
        DelayedProduceMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
   * Upon completion, return the current response status along with the error code per partition
   * 完成后，返回当前响应状态以及每个分区的错误代码
   */
  override def onComplete() {
    val responseStatus = produceMetadata.produceStatus.mapValues(status => status.responseStatus)
    //执行响应回调方法，KafkaApis#sendResponseCallback
    responseCallback(responseStatus)
  }
}

object DelayedProduceMetrics extends KafkaMetricsGroup {

  private val aggregateExpirationMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

  private val partitionExpirationMeterFactory = (key: TopicPartition) =>
    newMeter("ExpiresPerSec",
             "requests",
             TimeUnit.SECONDS,
             tags = Map("topic" -> key.topic, "partition" -> key.partition.toString))
  private val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

  def recordExpiration(partition: TopicPartition) {
    aggregateExpirationMeter.mark()
    partitionExpirationMeters.getAndMaybePut(partition).mark()
  }
}

