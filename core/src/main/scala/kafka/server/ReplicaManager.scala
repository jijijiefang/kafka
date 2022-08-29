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

import java.io.{File, IOException}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.yammer.metrics.core.Gauge
import kafka.api._
import kafka.cluster.{Partition, Replica}
import kafka.common._
import kafka.controller.KafkaController
import kafka.log.{LogAppendInfo, LogManager}
import kafka.message.{ByteBufferMessageSet, InvalidMessageException, Message, MessageSet}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._
import org.apache.kafka.common.errors.{ControllerMovedException, CorruptRecordException, InvalidTimestampException,
                                        InvalidTopicException, NotLeaderForPartitionException, OffsetOutOfRangeException,
                                        RecordBatchTooLargeException, RecordTooLargeException, ReplicaNotAvailableException,
                                        UnknownTopicOrPartitionException}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{LeaderAndIsrRequest, PartitionState, StopReplicaRequest, UpdateMetadataRequest}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.{Time => JTime}

import scala.collection._
import scala.collection.JavaConverters._

/*
 * Result metadata of a log append operation on the log
 * 日志上日志附加操作的结果元数据
 */
case class LogAppendResult(info: LogAppendInfo, error: Option[Throwable] = None) {
  def errorCode = error match {
    case None => Errors.NONE.code
    case Some(e) => Errors.forException(e).code
  }
}

/*
 * Result metadata of a log read operation on the log
 * 日志上日志读取操作的结果元数据
 * @param info @FetchDataInfo returned by the @Log read @Log read返回的info@FetchDataInfo
 * @param hw high watermark of the local replica 本地副本的高水位
 * @param readSize amount of data that was read from the log i.e. size of the fetch readSize从日志中读取的数据量，即获取的大小
 * @param isReadFromLogEnd true if the request read up to the log end offset snapshot isReadFromLogEnd true如果在启动读取时请求读取到日志结束偏移量快照，则为false，否则为false
 *                         when the read was initiated, false otherwise
 * @param error Exception if error encountered while reading from the log 如果从日志读取时遇到错误，则出现错误异常
 */
case class LogReadResult(info: FetchDataInfo,
                         hw: Long,
                         readSize: Int,
                         isReadFromLogEnd : Boolean,
                         error: Option[Throwable] = None) {

  def errorCode = error match {
    case None => Errors.NONE.code
    case Some(e) => Errors.forException(e).code
  }

  override def toString = {
    "Fetch Data: [%s], HW: [%d], readSize: [%d], isReadFromLogEnd: [%b], error: [%s]"
            .format(info, hw, readSize, isReadFromLogEnd, error)
  }
}

object LogReadResult {
  val UnknownLogReadResult = LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata,
                                                         MessageSet.Empty),
                                           -1L,
                                           -1,
                                           false)
}

case class BecomeLeaderOrFollowerResult(responseMap: collection.Map[TopicPartition, Short], errorCode: Short) {

  override def toString = {
    "update results: [%s], global error: [%d]".format(responseMap, errorCode)
  }
}

object ReplicaManager {
  val HighWatermarkFilename = "replication-offset-checkpoint"
  val IsrChangePropagationBlackOut = 5000L
  val IsrChangePropagationInterval = 60000L
}

/**
 * 副本管理器
 * @param config
 * @param metrics
 * @param time
 * @param jTime
 * @param zkUtils
 * @param scheduler
 * @param logManager
 * @param isShuttingDown
 * @param threadNamePrefix
 */
class ReplicaManager(val config: KafkaConfig,
                     metrics: Metrics,
                     time: Time,
                     jTime: JTime,
                     val zkUtils: ZkUtils,
                     scheduler: Scheduler,
                     val logManager: LogManager,
                     val isShuttingDown: AtomicBoolean,
                     threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {
  /* epoch of the controller that last changed the leader 上次更改Leader的控制器纪元*/
  @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  //本地BrokerId
  private val localBrokerId = config.brokerId
  private val allPartitions = new Pool[(String, Int), Partition](valueFactory = Some { case (t, p) =>
    new Partition(t, p, time, this)
  })
  //副本状态变化锁
  private val replicaStateChangeLock = new Object
  //副本拉取管理器
  val replicaFetcherManager = new ReplicaFetcherManager(config, this, metrics, jTime, threadNamePrefix)
  //HW校验点线程是否已启动
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  val highWatermarkCheckpoints = config.logDirs.map(dir => (new File(dir).getAbsolutePath, new OffsetCheckpoint(new File(dir, ReplicaManager.HighWatermarkFilename)))).toMap
  //HW线程是否初始化已完成初始化
  private var hwThreadInitialized = false
  this.logIdent = "[Replica Manager on Broker " + localBrokerId + "]: "
  val stateChangeLogger = KafkaController.stateChangeLogger
  //ISR变化集合
  private val isrChangeSet: mutable.Set[TopicAndPartition] = new mutable.HashSet[TopicAndPartition]()
  //上次ISR变化时间戳
  private val lastIsrChangeMs = new AtomicLong(System.currentTimeMillis())
  //上次ISR传播时间戳
  private val lastIsrPropagationMs = new AtomicLong(System.currentTimeMillis())
  //延迟生产炼狱
  val delayedProducePurgatory = DelayedOperationPurgatory[DelayedProduce](
    purgatoryName = "Produce", config.brokerId, config.producerPurgatoryPurgeIntervalRequests)
  //延迟拉取炼狱
  val delayedFetchPurgatory = DelayedOperationPurgatory[DelayedFetch](
    purgatoryName = "Fetch", config.brokerId, config.fetchPurgatoryPurgeIntervalRequests)

  val leaderCount = newGauge(
    "LeaderCount",
    new Gauge[Int] {
      def value = {
          getLeaderPartitions().size
      }
    }
  )
  val partitionCount = newGauge(
    "PartitionCount",
    new Gauge[Int] {
      def value = allPartitions.size
    }
  )
  val underReplicatedPartitions = newGauge(
    "UnderReplicatedPartitions",
    new Gauge[Int] {
      def value = underReplicatedPartitionCount()
    }
  )
  val isrExpandRate = newMeter("IsrExpandsPerSec",  "expands", TimeUnit.SECONDS)
  val isrShrinkRate = newMeter("IsrShrinksPerSec",  "shrinks", TimeUnit.SECONDS)

  def underReplicatedPartitionCount(): Int = {
      getLeaderPartitions().count(_.isUnderReplicated)
  }

  /**
   * 设置HW检查点线程启动状态
   */
  def startHighWaterMarksCheckPointThread() = {
    if(highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
      scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS)
  }

  /**
   * 记录ISR变化
   * @param topicAndPartition
   */
  def recordIsrChange(topicAndPartition: TopicAndPartition) {
    isrChangeSet synchronized {
      isrChangeSet += topicAndPartition
      lastIsrChangeMs.set(System.currentTimeMillis())
    }
  }
  /**
   * This function periodically runs to see if ISR needs to be propagated. It propagates ISR when: 此函数定期运行，以查看是否需要传播ISR，在以下情况下传播ISR：
   * 1. There is ISR change not propagated yet. 尚未传播ISR更改
   * 2. There is no ISR Change in the last five seconds, or it has been more than 60 seconds since the last ISR propagation. 在过去五秒钟内没有ISR变化，或者自上次ISR传播以来已超过60秒
   * This allows an occasional ISR change to be propagated within a few seconds, and avoids overwhelming controller and
   * other brokers when large amount of ISR change occurs. 这允许在几秒钟内传播偶尔的ISR更改，并避免在发生大量ISR更改时压倒控制器和其他代理
   */
  def maybePropagateIsrChanges() {
    val now = System.currentTimeMillis()
    isrChangeSet synchronized {
      if (isrChangeSet.nonEmpty && //尚未传播ISR更改
        (lastIsrChangeMs.get() + ReplicaManager.IsrChangePropagationBlackOut < now || //在过去五秒钟内没有ISR变化
          lastIsrPropagationMs.get() + ReplicaManager.IsrChangePropagationInterval < now)) {//自上次ISR传播以来已超过60秒
        //传播ISR变化
        ReplicationUtils.propagateIsrChanges(zkUtils, isrChangeSet)
        //清空
        isrChangeSet.clear()
        lastIsrPropagationMs.set(now)
      }
    }
  }

  /**
   * Try to complete some delayed produce requests with the request key;
   * this can be triggered when: 尝试完成延迟生产请求当这些请求时触发
   *
   * 1. The partition HW has changed (for acks = -1) 如果acks = -1,分区HW发生变化
   * 2. A follower replica's fetch operation is received (for acks > 1) 如果acks > 1,接收到跟随者副本的拉取操作
   */
  def tryCompleteDelayedProduce(key: DelayedOperationKey) {
    val completed = delayedProducePurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d producer requests.".format(key.keyLabel, completed))
  }

  /**
   * Try to complete some delayed fetch requests with the request key; 尝试完成某些延迟拉取请求操作使用请求键
   * this can be triggered when: 可能由某些情况触发
   *
   * 1. The partition HW has changed (for regular fetch) 分区HW发生了变化
   * 2. A new message set is appended to the local log (for follower fetch) 一个新的消息集被追加到本地日志
   */
  def tryCompleteDelayedFetch(key: DelayedOperationKey) {
    val completed = delayedFetchPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d fetch requests.".format(key.keyLabel, completed))
  }

  /**
   * 启动
   */
  def startup() {
    // start ISR expiration thread 启动ISR过期线程
    scheduler.schedule("isr-expiration", maybeShrinkIsr, period = config.replicaLagTimeMaxMs, unit = TimeUnit.MILLISECONDS)
    //启动ISR变化传播线程
    scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges, period = 2500L, unit = TimeUnit.MILLISECONDS)
  }

  /**
   * 停止复制
   * @param topic
   * @param partitionId
   * @param deletePartition
   * @return
   */
  def stopReplica(topic: String, partitionId: Int, deletePartition: Boolean): Short  = {
    stateChangeLogger.trace("Broker %d handling stop replica (delete=%s) for partition [%s,%d]".format(localBrokerId,
      deletePartition.toString, topic, partitionId))
    val errorCode = Errors.NONE.code
    getPartition(topic, partitionId) match {
      case Some(partition) =>
        if(deletePartition) {
          val removedPartition = allPartitions.remove((topic, partitionId))
          if (removedPartition != null) {
            removedPartition.delete() // this will delete the local log
            val topicHasPartitions = allPartitions.keys.exists { case (t, _) => topic == t }
            if (!topicHasPartitions)
                BrokerTopicStats.removeMetrics(topic)
          }
        }
      case None =>
        // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
        // This could happen when topic is being deleted while broker is down and recovers.
        if(deletePartition) {
          val topicAndPartition = TopicAndPartition(topic, partitionId)

          if(logManager.getLog(topicAndPartition).isDefined) {
              logManager.deleteLog(topicAndPartition)
          }
        }
        stateChangeLogger.trace("Broker %d ignoring stop replica (delete=%s) for partition [%s,%d] as replica doesn't exist on broker"
          .format(localBrokerId, deletePartition, topic, partitionId))
    }
    stateChangeLogger.trace("Broker %d finished handling stop replica (delete=%s) for partition [%s,%d]"
      .format(localBrokerId, deletePartition, topic, partitionId))
    errorCode
  }

  /**
   * 停止复制
   * @param stopReplicaRequest 停止复制请求
   * @return
   */
  def stopReplicas(stopReplicaRequest: StopReplicaRequest): (mutable.Map[TopicPartition, Short], Short) = {
    replicaStateChangeLock synchronized {
      val responseMap = new collection.mutable.HashMap[TopicPartition, Short]
      if(stopReplicaRequest.controllerEpoch() < controllerEpoch) {
        stateChangeLogger.warn("Broker %d received stop replica request from an old controller epoch %d. Latest known controller epoch is %d"
          .format(localBrokerId, stopReplicaRequest.controllerEpoch, controllerEpoch))
        (responseMap, Errors.STALE_CONTROLLER_EPOCH.code)
      } else {
        val partitions = stopReplicaRequest.partitions.asScala
        controllerEpoch = stopReplicaRequest.controllerEpoch
        // First stop fetchers for all partitions, then stop the corresponding replicas
        replicaFetcherManager.removeFetcherForPartitions(partitions.map(r => TopicAndPartition(r.topic, r.partition)))
        for(topicPartition <- partitions){
          val errorCode = stopReplica(topicPartition.topic, topicPartition.partition, stopReplicaRequest.deletePartitions)
          responseMap.put(topicPartition, errorCode)
        }
        (responseMap, Errors.NONE.code)
      }
    }
  }

  def getOrCreatePartition(topic: String, partitionId: Int): Partition = {
    allPartitions.getAndMaybePut((topic, partitionId))
  }

  def getPartition(topic: String, partitionId: Int): Option[Partition] = {
    val partition = allPartitions.get((topic, partitionId))
    if (partition == null)
      None
    else
      Some(partition)
  }

  def getReplicaOrException(topic: String, partition: Int): Replica = {
    val replicaOpt = getReplica(topic, partition)
    if(replicaOpt.isDefined)
      replicaOpt.get
    else
      throw new ReplicaNotAvailableException("Replica %d is not available for partition [%s,%d]".format(config.brokerId, topic, partition))
  }

  /**
   * 获取此主题分区的本地Leader副本
   * @param topic 主题
   * @param partitionId 分区
   * @return
   */
  def getLeaderReplicaIfLocal(topic: String, partitionId: Int): Replica =  {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case None =>
        throw new UnknownTopicOrPartitionException("Partition [%s,%d] doesn't exist on %d".format(topic, partitionId, config.brokerId))
      case Some(partition) =>
        partition.leaderReplicaIfLocal match {
          case Some(leaderReplica) => leaderReplica
          case None =>
            throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
                                                     .format(topic, partitionId, config.brokerId))
        }
    }
  }

  /**
   * 根据主题分区获取副本
   * @param topic 主题
   * @param partitionId 分区
   * @param replicaId 副本
   * @return
   */
  def getReplica(topic: String, partitionId: Int, replicaId: Int = config.brokerId): Option[Replica] =  {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case None => None
      case Some(partition) => partition.getReplica(replicaId)
    }
  }

  /**
   * Append messages to leader replicas of the partition, and wait for them to be replicated to other replicas;
   * the callback function will be triggered either when timeout or the required acks are satisfied
   * 将消息附加到分区的Leader副本，并等待它们被复制到其他副本；当超时或满足所需的确认时，将触发回调函数
   */
  def appendMessages(timeout: Long,
                     requiredAcks: Short,
                     internalTopicsAllowed: Boolean,
                     messagesPerPartition: Map[TopicPartition, MessageSet],
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit) {
    //校验acks参数是否正确
    if (isValidRequiredAcks(requiredAcks)) {
      val sTime = SystemTime.milliseconds
      //追加消息至本地文件
      val localProduceResults = appendToLocalLog(internalTopicsAllowed, messagesPerPartition, requiredAcks)
      debug("Produce to local log in %d ms".format(SystemTime.milliseconds - sTime))

      val produceStatus = localProduceResults.map { case (topicPartition, result) =>
        topicPartition ->
                ProducePartitionStatus(
                  result.info.lastOffset + 1, // required offset 所需偏移量
                  new PartitionResponse(result.errorCode, result.info.firstOffset, result.info.timestamp)) // response status 响应状态
      }
      //如果需要创建延迟请求
      if (delayedRequestRequired(requiredAcks, messagesPerPartition, localProduceResults)) {
        // create delayed produce operation 创建延迟生产操作
        val produceMetadata = ProduceMetadata(requiredAcks, produceStatus)
        //延迟生产操作
        val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback)

        // create a list of (topic, partition) pairs to use as keys for this delayed produce operation 创建（主题、分区）对的列表，用作此延迟生成操作的键
        val producerRequestKeys = messagesPerPartition.keys.map(new TopicPartitionOperationKey(_)).toSeq

        // try to complete the request immediately, otherwise put it into the purgatory
        // this is because while the delayed produce operation is being created, new
        // requests may arrive and hence make this operation completable. 尝试立即完成请求，否则将其放入炼狱。这是因为在创建延迟的生产操作时，新请求可能会到达，从而使此操作可完成
        delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)

      } else {
        // we can respond immediately 我们可以立即作出响应
        val produceResponseStatus = produceStatus.mapValues(status => status.responseStatus)
        responseCallback(produceResponseStatus)
      }
    } else {
      // If required.acks is outside accepted range, something is wrong with the client
      // Just return an error and don't handle the request at all
      //如果required.acks超出了可接受的范围，则客户端出现问题，只需返回一个错误，根本不处理请求
      val responseStatus = messagesPerPartition.map {
        case (topicAndPartition, messageSet) =>
          (topicAndPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS.code,
                                                      LogAppendInfo.UnknownLogAppendInfo.firstOffset,
                                                      Message.NoTimestamp))
      }
      responseCallback(responseStatus)
    }
  }

  // If all the following conditions are true, we need to put a delayed produce request and wait for replication to complete
  // 如果以下所有条件均为真，则需要发出延迟的生产请求并等待复制完成
  // 1. required acks = -1 如果acks是-1
  // 2. there is data to append 有数据要追加
  // 3. at least one partition append was successful (fewer errors than partitions) 至少有一个分区追加成功（错误少于分区）
  private def delayedRequestRequired(requiredAcks: Short, messagesPerPartition: Map[TopicPartition, MessageSet],
                                       localProduceResults: Map[TopicPartition, LogAppendResult]): Boolean = {
    requiredAcks == -1 &&
    messagesPerPartition.size > 0 &&
    localProduceResults.values.count(_.error.isDefined) < messagesPerPartition.size
  }

  private def isValidRequiredAcks(requiredAcks: Short): Boolean = {
    requiredAcks == -1 || requiredAcks == 1 || requiredAcks == 0
  }

  /**
   * Append the messages to the local replica logs
   * 将消息附加到本地副本日志
   */
  private def appendToLocalLog(internalTopicsAllowed: Boolean,
                               messagesPerPartition: Map[TopicPartition, MessageSet],
                               requiredAcks: Short): Map[TopicPartition, LogAppendResult] = {
    trace("Append [%s] to local log ".format(messagesPerPartition))
    messagesPerPartition.map { case (topicPartition, messages) =>
      BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).totalProduceRequestRate.mark()
      BrokerTopicStats.getBrokerAllTopicsStats().totalProduceRequestRate.mark()

      // reject appending to internal topics if it is not allowed 如果不允许,拒绝追加至内部主题
      if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
        (topicPartition, LogAppendResult(
          LogAppendInfo.UnknownLogAppendInfo,
          Some(new InvalidTopicException("Cannot append to internal topic %s".format(topicPartition.topic)))))
      } else {
        try {
          //获取主题分区
          val partitionOpt = getPartition(topicPartition.topic, topicPartition.partition)
          val info = partitionOpt match {
            case Some(partition) =>
              //调用partition的appendMessagesToLeader,batch数据写入磁盘
              partition.appendMessagesToLeader(messages.asInstanceOf[ByteBufferMessageSet], requiredAcks)
            case None => throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d"
              .format(topicPartition, localBrokerId))
          }

          val numAppendedMessages =
            if (info.firstOffset == -1L || info.lastOffset == -1L)
              0
            else
              info.lastOffset - info.firstOffset + 1

          // update stats for successfully appended bytes and messages as bytesInRate and messageInRate 将成功附加的字节和消息的统计信息更新为bytesInRate和messageInRate
          BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).bytesInRate.mark(messages.sizeInBytes)
          BrokerTopicStats.getBrokerAllTopicsStats.bytesInRate.mark(messages.sizeInBytes)
          BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
          BrokerTopicStats.getBrokerAllTopicsStats.messagesInRate.mark(numAppendedMessages)

          trace("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d"
            .format(messages.sizeInBytes, topicPartition.topic, topicPartition.partition, info.firstOffset, info.lastOffset))
          (topicPartition, LogAppendResult(info))
        } catch {
          // NOTE: Failed produce requests metric is not incremented for known exceptions
          // it is supposed to indicate un-expected failures of a broker in handling a produce request
          case e: KafkaStorageException =>
            fatal("Halting due to unrecoverable I/O error while handling produce request: ", e)
            Runtime.getRuntime.halt(1)
            (topicPartition, null)
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderForPartitionException |
                   _: RecordTooLargeException |
                   _: RecordBatchTooLargeException |
                   _: CorruptRecordException |
                   _: InvalidMessageException |
                   _: InvalidTimestampException) =>
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
          case t: Throwable =>
            BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).failedProduceRequestRate.mark()
            BrokerTopicStats.getBrokerAllTopicsStats.failedProduceRequestRate.mark()
            error("Error processing append operation on partition %s".format(topicPartition), t)
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(t)))
        }
      }
    }
  }

  /**
   * Fetch messages from the leader replica, and wait until enough data can be fetched and return;
   * the callback function will be triggered either when timeout or required fetch info is satisfied
   * 从Leader副本获取消息，并等待足够的数据能够获取并返回；当超时或满足所需的获取信息时，将触发回调函数
   */
  def fetchMessages(timeout: Long,
                    replicaId: Int,
                    fetchMinBytes: Int,
                    fetchInfo: immutable.Map[TopicAndPartition, PartitionFetchInfo],
                    responseCallback: Map[TopicAndPartition, FetchResponsePartitionData] => Unit) {
    //replicaId如果大于等于0说明来自Follower，否则来自于消费者
    val isFromFollower = replicaId >= 0
    //是否只从Leader拉取，条件是非debug模式
    val fetchOnlyFromLeader: Boolean = replicaId != Request.DebuggingConsumerId
    //是否只拉取已提交的,条件是BrokerId非负整数
    val fetchOnlyCommitted: Boolean = ! Request.isValidBrokerId(replicaId)

    // read from local logs 从本地日志文件读取
    val logReadResults = readFromLocalLog(fetchOnlyFromLeader, fetchOnlyCommitted, fetchInfo)

    // if the fetch comes from the follower,
    // update its corresponding log end offset 如果拉取来自Follower，则更新其相应的LEO
    if(Request.isValidBrokerId(replicaId))
      updateFollowerLogReadResults(replicaId, logReadResults)//更新跟随者副本日志读取结果,更新Follower副本LEO和lastCaughtUpTimeMs

    // check if this fetch request can be satisfied right away 检查是否可以立即满足此拉取请求
    val bytesReadable = logReadResults.values.map(_.info.messageSet.sizeInBytes).sum
    val errorReadingData = logReadResults.values.foldLeft(false) ((errorIncurred, readResult) =>
      errorIncurred || (readResult.errorCode != Errors.NONE.code))

    // respond immediately if 1) fetch request does not want to wait 如果 1.提取请求不想等待，请立即响应
    //                        2) fetch request does not require any data 2.提取请求不需要任何数据
    //                        3) has enough data to respond 3.有足够的数据来响应
    //                        4) some error happens while reading data 4.读取数据时发生错误
    if(timeout <= 0 || fetchInfo.size <= 0 || bytesReadable >= fetchMinBytes || errorReadingData) {
      //拉取的数据
      val fetchPartitionData = logReadResults.mapValues(result =>
        FetchResponsePartitionData(result.errorCode, result.hw, result.info.messageSet))
      responseCallback(fetchPartitionData)//拉取响应回调
    } else {
      // construct the fetch results from the read results 从读取结果构造获取结果
      val fetchPartitionStatus = logReadResults.map { case (topicAndPartition, result) =>
        (topicAndPartition, FetchPartitionStatus(result.info.fetchOffsetMetadata, fetchInfo.get(topicAndPartition).get))
      }
      val fetchMetadata = FetchMetadata(fetchMinBytes, fetchOnlyFromLeader, fetchOnlyCommitted, isFromFollower, fetchPartitionStatus)
      val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation 创建（主题、分区）对的列表，用作此延迟获取操作的键
      val delayedFetchKeys = fetchPartitionStatus.keys.map(new TopicPartitionOperationKey(_)).toSeq

      // try to complete the request immediately, otherwise put it into the purgatory; 尝试立即完成请求，否则将其放入炼狱；
      // this is because while the delayed fetch operation is being created, new requests 这是因为在创建延迟的获取操作时，可能会出现新的请求，从而使此操作变得可完成
      // may arrive and hence make this operation completable.
      delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)//延迟拉取炼狱尝试完成延迟抓取操作
    }
  }

  /**
   * Read from a single topic/partition at the given offset upto maxSize bytes
   * 以给定偏移量从单个主题/分区读取最大字节数
   */
  def readFromLocalLog(fetchOnlyFromLeader: Boolean,
                       readOnlyCommitted: Boolean,
                       readPartitionInfo: Map[TopicAndPartition, PartitionFetchInfo]): Map[TopicAndPartition, LogReadResult] = {

    readPartitionInfo.map { case (TopicAndPartition(topic, partition), PartitionFetchInfo(offset, fetchSize)) =>
      BrokerTopicStats.getBrokerTopicStats(topic).totalFetchRequestRate.mark()
      BrokerTopicStats.getBrokerAllTopicsStats().totalFetchRequestRate.mark()

      val partitionDataAndOffsetInfo =
        try {
          trace("Fetching log segment for topic %s, partition %d, offset %d, size %d".format(topic, partition, offset, fetchSize))

          // decide whether to only fetch from leader 决定是否仅从领导处获取
          val localReplica = if (fetchOnlyFromLeader)
            getLeaderReplicaIfLocal(topic, partition)
          else
            getReplicaOrException(topic, partition)

          // decide whether to only fetch committed data (i.e. messages below high watermark) 决定是否仅获取提交的数据（即低于高水位线的消息）
          val maxOffsetOpt = if (readOnlyCommitted)
            Some(localReplica.highWatermark.messageOffset)
          else
            None

          /* Read the LogOffsetMetadata prior to performing the read from the log.在从日志执行读取之前，读取LogOffsetMetadata。
           * We use the LogOffsetMetadata to determine if a particular replica is in-sync or not. 我们使用LogOffsetMetadata来确定特定副本是否同步。
           * Using the log end offset after performing the read can lead to a race condition 执行读取后使用日志结束偏移量可能会导致争用情况，
           * where data gets appended to the log immediately after the replica has consumed from it 即在复制副本从日志中消耗数据后，数据会立即附加到日志中。
           * This can cause a replica to always be out of sync. 这可能会导致复制副本始终不同步。
           */
          val initialLogEndOffset = localReplica.logEndOffset
          val logReadInfo = localReplica.log match {
            case Some(log) =>
              //从文件读取数据
              log.read(offset, fetchSize, maxOffsetOpt)
            case None =>
              error("Leader for partition [%s,%d] does not have a local log".format(topic, partition))
              FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty)
          }

          val readToEndOfLog = initialLogEndOffset.messageOffset - logReadInfo.fetchOffsetMetadata.messageOffset <= 0
          //构建日志读取结果
          LogReadResult(logReadInfo, localReplica.highWatermark.messageOffset, fetchSize, readToEndOfLog, None)
        } catch {
          // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
          // is supposed to indicate un-expected failure of a broker in handling a fetch request
          case utpe: UnknownTopicOrPartitionException =>
            LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty), -1L, fetchSize, false, Some(utpe))
          case nle: NotLeaderForPartitionException =>
            LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty), -1L, fetchSize, false, Some(nle))
          case rnae: ReplicaNotAvailableException =>
            LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty), -1L, fetchSize, false, Some(rnae))
          case oor : OffsetOutOfRangeException =>
            LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty), -1L, fetchSize, false, Some(oor))
          case e: Throwable =>
            BrokerTopicStats.getBrokerTopicStats(topic).failedFetchRequestRate.mark()
            BrokerTopicStats.getBrokerAllTopicsStats().failedFetchRequestRate.mark()
            error("Error processing fetch operation on partition [%s,%d] offset %d".format(topic, partition, offset), e)
            LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty), -1L, fetchSize, false, Some(e))
        }
      (TopicAndPartition(topic, partition), partitionDataAndOffsetInfo)
    }
  }

  def getMessageFormatVersion(topicAndPartition: TopicAndPartition): Option[Byte] =
    getReplica(topicAndPartition.topic, topicAndPartition.partition).flatMap { replica =>
      replica.log.map(_.config.messageFormatVersion.messageFormatVersion)
    }

  /**
   * 可能需要更新元数据缓存
   * @param correlationId
   * @param updateMetadataRequest
   * @param metadataCache
   */
  def maybeUpdateMetadataCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest, metadataCache: MetadataCache) {
    replicaStateChangeLock synchronized {
      //如果请求控制器纪元小于当前控制器纪元
      if(updateMetadataRequest.controllerEpoch < controllerEpoch) {
        val stateControllerEpochErrorMessage = ("Broker %d received update metadata request with correlation id %d from an " +
          "old controller %d with epoch %d. Latest known controller epoch is %d").format(localBrokerId,
          correlationId, updateMetadataRequest.controllerId, updateMetadataRequest.controllerEpoch,
          controllerEpoch)
        stateChangeLogger.warn(stateControllerEpochErrorMessage)
        throw new ControllerMovedException(stateControllerEpochErrorMessage)
      } else {
        //更新缓存
        metadataCache.updateCache(correlationId, updateMetadataRequest)
        //更新控制器纪元
        controllerEpoch = updateMetadataRequest.controllerEpoch
      }
    }
  }

  /**
   * 成为Leader或Follower
   * @param correlationId
   * @param leaderAndISRRequest
   * @param metadataCache
   * @param onLeadershipChange
   * @return
   */
  def becomeLeaderOrFollower(correlationId: Int,leaderAndISRRequest: LeaderAndIsrRequest,
                             metadataCache: MetadataCache,
                             onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): BecomeLeaderOrFollowerResult = {
    leaderAndISRRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
      stateChangeLogger.trace("Broker %d received LeaderAndIsr request %s correlation id %d from controller %d epoch %d for partition [%s,%d]"
                                .format(localBrokerId, stateInfo, correlationId,
                                        leaderAndISRRequest.controllerId, leaderAndISRRequest.controllerEpoch, topicPartition.topic, topicPartition.partition))
    }
    replicaStateChangeLock synchronized {
      val responseMap = new mutable.HashMap[TopicPartition, Short]
      //请求的控制器纪元小于控制器纪元
      if (leaderAndISRRequest.controllerEpoch < controllerEpoch) {
        //忽略此请求
        leaderAndISRRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
        stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d since " +
          "its controller epoch %d is old. Latest known controller epoch is %d").format(localBrokerId, leaderAndISRRequest.controllerId,
          correlationId, leaderAndISRRequest.controllerEpoch, controllerEpoch))
        }
        //返回异常
        BecomeLeaderOrFollowerResult(responseMap, Errors.STALE_CONTROLLER_EPOCH.code)
      } else {
        val controllerId = leaderAndISRRequest.controllerId
        controllerEpoch = leaderAndISRRequest.controllerEpoch

        // First check partition's leader epoch 首先检查分区的Leader纪元
        val partitionState = new mutable.HashMap[Partition, PartitionState]()
        leaderAndISRRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
          val partition = getOrCreatePartition(topicPartition.topic, topicPartition.partition)
          val partitionLeaderEpoch = partition.getLeaderEpoch()
          // If the leader epoch is valid record the epoch of the controller that made the leadership decision. 如果领导历元有效，则记录做出领导决策的控制器的历元。
          // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path 这在更新isr以在zookeeper路径中维护决策者控制器的历元时非常有用
          //当前Leader纪元小于请求Leader纪元
          if (partitionLeaderEpoch < stateInfo.leaderEpoch) {
            if(stateInfo.replicas.contains(config.brokerId))
              partitionState.put(partition, stateInfo)
            else {
              stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
                "epoch %d for partition [%s,%d] as itself is not in assigned replica list %s")
                .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                  topicPartition.topic, topicPartition.partition, stateInfo.replicas.asScala.mkString(",")))
              responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
            }
          } else {
            // Otherwise record the error code in response 否则，在响应中记录错误代码
            stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
              "epoch %d for partition [%s,%d] since its associated leader epoch %d is old. Current leader epoch is %d")
              .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                topicPartition.topic, topicPartition.partition, stateInfo.leaderEpoch, partitionLeaderEpoch))
            responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH.code)
          }
        }
        //分区副本要变成Leader的集合
        val partitionsTobeLeader = partitionState.filter { case (partition, stateInfo) =>
          //筛选出副本Leader是当前brokerId
          stateInfo.leader == config.brokerId
        }
        //分区副本要变成Follower的集合
        val partitionsToBeFollower = (partitionState -- partitionsTobeLeader.keys)

        val partitionsBecomeLeader = if (!partitionsTobeLeader.isEmpty)
          makeLeaders(controllerId, controllerEpoch, partitionsTobeLeader, correlationId, responseMap)//成为Leader
        else
          Set.empty[Partition]
        val partitionsBecomeFollower = if (!partitionsToBeFollower.isEmpty)
          makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap, metadataCache)//成为Follower
        else
          Set.empty[Partition]

        // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions 我们在第一个请求之后初始化HW线程。
        // have been completely populated before starting the checkpointing there by avoiding weird race conditions 这通过避免奇怪的竞争条件，确保在启动检查点之前，所有分区都已完全填充
        if (!hwThreadInitialized) {
          //设置HW检查点线程启动状态
          startHighWaterMarksCheckPointThread()
          hwThreadInitialized = true
        }
        replicaFetcherManager.shutdownIdleFetcherThreads()
        //调用传入方法
        onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)
        //组装结果
        BecomeLeaderOrFollowerResult(responseMap, Errors.NONE.code)
      }
    }
  }

  /*
   * Make the current broker to become leader for a given set of partitions by:
   * 通过以下方式使当前代理成为给定分区集的领导者：
   * 1. Stop fetchers for these partitions 停止这些分区的抓取程序
   * 2. Update the partition metadata in cache 更新缓存中的分区元数据
   * 3. Add these partitions to the leader partitions set 将这些分区添加到Leader分区集中
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made leader due to this method
   * 如果此函数中抛出意外错误，它将传播到KafkaApis，在那里，错误消息将在每个分区上设置，因为我们不知道是哪个分区导致了错误。否则，返回由于此方法而成为Leader的分区集
   *  TODO: the above may need to be fixed later
   */
  private def makeLeaders(controllerId: Int,
                          epoch: Int,
                          partitionState: Map[Partition, PartitionState],
                          correlationId: Int,
                          responseMap: mutable.Map[TopicPartition, Short]): Set[Partition] = {
    partitionState.foreach(state =>
      stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "starting the become-leader transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId))))

    for (partition <- partitionState.keys)
      responseMap.put(new TopicPartition(partition.topic, partition.partitionId), Errors.NONE.code)

    val partitionsToMakeLeaders: mutable.Set[Partition] = mutable.Set()

    try {
      // First stop fetchers for all the partitions 首先停止所有分区的拉取线程
      replicaFetcherManager.removeFetcherForPartitions(partitionState.keySet.map(new TopicAndPartition(_)))
      // Update the partition information to be the leader 更新分区信息，使其成为Leader
      partitionState.foreach{ case (partition, partitionStateInfo) =>
        if (partition.makeLeader(controllerId, partitionStateInfo, correlationId))//如果本地分区成为Leader
          partitionsToMakeLeaders += partition
        else
          stateChangeLogger.info(("Broker %d skipped the become-leader state change after marking its partition as leader with correlation id %d from " +
            "controller %d epoch %d for partition %s since it is already the leader for the partition.")
            .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(partition.topic, partition.partitionId)));
      }
      partitionsToMakeLeaders.foreach { partition =>
        stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-leader request from controller " +
          "%d epoch %d with correlation id %d for partition %s")
          .format(localBrokerId, controllerId, epoch, correlationId, TopicAndPartition(partition.topic, partition.partitionId)))
      }
    } catch {
      case e: Throwable =>
        partitionState.foreach { state =>
          val errorMsg = ("Error on broker %d while processing LeaderAndIsr request correlationId %d received from controller %d" +
            " epoch %d for partition %s").format(localBrokerId, correlationId, controllerId, epoch,
                                                TopicAndPartition(state._1.topic, state._1.partitionId))
          stateChangeLogger.error(errorMsg, e)
        }
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionState.foreach { state =>
      stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "for the become-leader transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
    }

    partitionsToMakeLeaders
  }

  /*
   * Make the current broker to become follower for a given set of partitions by:
   * 通过以下方法使当前代理成为给定分区集的跟随者：
   * 1. Remove these partitions from the leader partitions set. 从Leader分区集中删除这些分区
   * 2. Mark the replicas as followers so that no more data can be added from the producer clients. 将复制副本标记为跟随者，以便无法从生产者客户端添加更多数据
   * 3. Stop fetchers for these partitions so that no more data can be added by the replica fetcher threads. 停止这些分区的提取程序，以便副本提取程序线程不能添加更多数据
   * 4. Truncate the log and checkpoint offsets for these partitions. 截断这些分区的日志和检查点偏移
   * 5. Clear the produce and fetch requests in the purgatory 清除炼狱中的product和fetch请求
   * 6. If the broker is not shutting down, add the fetcher to the new leaders. 如果代理未关闭，请将抓取程序添加到新的领导者
   *
   * The ordering of doing these steps make sure that the replicas in transition will not
   * take any more messages before checkpointing offsets so that all messages before the checkpoint
   * are guaranteed to be flushed to disks
   * 执行这些步骤的顺序确保转换中的复制副本在检查点偏移之前不会再接收任何消息，以便确保将检查点之前的所有消息刷新到磁盘
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made follower due to this method
   * 如果此函数中抛出意外错误，它将传播到KafkaApis，在那里，错误消息将在每个分区上设置，因为我们不知道是哪个分区导致了错误。否则，返回由于此方法而成为跟随者的分区集
   */
  private def makeFollowers(controllerId: Int,
                            epoch: Int,
                            partitionState: Map[Partition, PartitionState],
                            correlationId: Int,
                            responseMap: mutable.Map[TopicPartition, Short],
                            metadataCache: MetadataCache) : Set[Partition] = {
    partitionState.foreach { state =>
      stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "starting the become-follower transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
    }

    for (partition <- partitionState.keys)
      responseMap.put(new TopicPartition(partition.topic, partition.partitionId), Errors.NONE.code)

    val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()

    try {

      // TODO: Delete leaders from LeaderAndIsrRequest
      partitionState.foreach{ case (partition, partitionStateInfo) =>
        val newLeaderBrokerId = partitionStateInfo.leader
        //元数据缓存筛选可用新的Leader的BrokerId
        metadataCache.getAliveBrokers.find(_.id == newLeaderBrokerId) match {
          // Only change partition state when the leader is available 仅在Leader可用时更改分区状态
          case Some(leaderBroker) =>
            if (partition.makeFollower(controllerId, partitionStateInfo, correlationId))//当前分区副本作为Follower
              partitionsToMakeFollower += partition
            else
              stateChangeLogger.info(("Broker %d skipped the become-follower state change after marking its partition as follower with correlation id %d from " +
                "controller %d epoch %d for partition [%s,%d] since the new leader %d is the same as the old leader")
                .format(localBrokerId, correlationId, controllerId, partitionStateInfo.controllerEpoch,
                partition.topic, partition.partitionId, newLeaderBrokerId))
          case None =>
            // The leader broker should always be present in the metadata cache. 领导者代理应始终存在于元数据缓存中。
            // If not, we should record the error message and abort the transition process for this partition 如果没有，我们应该记录错误消息并中止该分区的转换过程
            stateChangeLogger.error(("Broker %d received LeaderAndIsrRequest with correlation id %d from controller" +
              " %d epoch %d for partition [%s,%d] but cannot become follower since the new leader %d is unavailable.")
              .format(localBrokerId, correlationId, controllerId, partitionStateInfo.controllerEpoch,
              partition.topic, partition.partitionId, newLeaderBrokerId))
            // Create the local replica even if the leader is unavailable. This is required to ensure that we include 即使Leader不可用，也要创建本地副本。
            // the partition's high watermark in the checkpoint file (see KAFKA-1647) 这是确保在检查点文件中包含分区的高水位线所必需的
            partition.getOrCreateReplica()
        }
      }

      replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(new TopicAndPartition(_)))
      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-follower request from controller " +
          "%d epoch %d with correlation id %d for partition %s")
          .format(localBrokerId, controllerId, epoch, correlationId, TopicAndPartition(partition.topic, partition.partitionId)))
      }

      logManager.truncateTo(partitionsToMakeFollower.map(partition => (new TopicAndPartition(partition), partition.getOrCreateReplica().highWatermark.messageOffset)).toMap)
      partitionsToMakeFollower.foreach { partition =>
        val topicPartitionOperationKey = new TopicPartitionOperationKey(partition.topic, partition.partitionId)
        //尝试完成延迟生产消息
        tryCompleteDelayedProduce(topicPartitionOperationKey)
        //尝试完成延迟拉取消息
        tryCompleteDelayedFetch(topicPartitionOperationKey)
      }

      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(("Broker %d truncated logs and checkpointed recovery boundaries for partition [%s,%d] as part of " +
          "become-follower request with correlation id %d from controller %d epoch %d").format(localBrokerId,
          partition.topic, partition.partitionId, correlationId, controllerId, epoch))
      }

      if (isShuttingDown.get()) {
        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(("Broker %d skipped the adding-fetcher step of the become-follower state change with correlation id %d from " +
            "controller %d epoch %d for partition [%s,%d] since it is shutting down").format(localBrokerId, correlationId,
            controllerId, epoch, partition.topic, partition.partitionId))
        }
      }
      else {
        // we do not need to check if the leader exists again since this has been done at the beginning of this process 我们不需要检查领导者是否再次存在，因为这是在流程开始时完成的
        val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map(partition =>
          //构建主题分区：Broker和初始化偏移量
          new TopicAndPartition(partition) -> BrokerAndInitialOffset(
            //获取存活的Broker的端点，Leader副本所在Broker
            metadataCache.getAliveBrokers.find(_.id == partition.leaderReplicaIdOpt.get).get.getBrokerEndPoint(config.interBrokerSecurityProtocol),
            //偏移量
            partition.getReplica().get.logEndOffset.messageOffset)).toMap
        //为分区添加抓取线程
        replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)

        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(("Broker %d started fetcher to new leader as part of become-follower request from controller " +
            "%d epoch %d with correlation id %d for partition [%s,%d]")
            .format(localBrokerId, controllerId, epoch, correlationId, partition.topic, partition.partitionId))
        }
      }
    } catch {
      case e: Throwable =>
        val errorMsg = ("Error on broker %d while processing LeaderAndIsr request with correlationId %d received from controller %d " +
          "epoch %d").format(localBrokerId, correlationId, controllerId, epoch)
        stateChangeLogger.error(errorMsg, e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionState.foreach { state =>
      stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "for the become-follower transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
    }

    partitionsToMakeFollower
  }

  /**
   * 可能收缩ISR
   */
  private def maybeShrinkIsr(): Unit = {
    //评估分区的ISR列表，以查看可以从ISR中删除哪些复制副本
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")
    //"replica.lag.time.max.ms"默认10S
    allPartitions.values.foreach(partition => partition.maybeShrinkIsr(config.replicaLagTimeMaxMs))
  }

  /**
   * 更新跟随者副本日志读取结果
   * @param replicaId
   * @param readResults
   */
  private def updateFollowerLogReadResults(replicaId: Int, readResults: Map[TopicAndPartition, LogReadResult]) {
    debug("Recording follower broker %d log read results: %s ".format(replicaId, readResults))
    readResults.foreach { case (topicAndPartition, readResult) =>
      getPartition(topicAndPartition.topic, topicAndPartition.partition) match {
        case Some(partition) =>
          //更新分区副本的LEO和lastCaughtUpTimeMs
          partition.updateReplicaLogReadResult(replicaId, readResult)

          // for producer requests with ack > 1, we need to check 对于ack>1的生产者请求，我们需要检查
          // if they can be unblocked after some follower's log end offsets have moved 在某些Follower的LEO移动后是否可以解除阻塞
          //尝试完成延迟生产消息操作
          tryCompleteDelayedProduce(new TopicPartitionOperationKey(topicAndPartition))
        case None =>
          warn("While recording the replica LEO, the partition %s hasn't been created.".format(topicAndPartition))
      }
    }
  }

  private def getLeaderPartitions() : List[Partition] = {
    allPartitions.values.filter(_.leaderReplicaIfLocal().isDefined).toList
  }

  /**
   * Flushes the highwatermark value for all partitions to the highwatermark file
   * 将所有分区的highwatermark值刷新到highwatermark文件
   */
  def checkpointHighWatermarks() {
    val replicas = allPartitions.values.flatMap(_.getReplica(config.brokerId))
    val replicasByDir = replicas.filter(_.log.isDefined).groupBy(_.log.get.dir.getParentFile.getAbsolutePath)
    for ((dir, reps) <- replicasByDir) {
      val hwms = reps.map(r => new TopicAndPartition(r) -> r.highWatermark.messageOffset).toMap
      try {
        highWatermarkCheckpoints(dir).write(hwms)
      } catch {
        case e: IOException =>
          fatal("Error writing to highwatermark file: ", e)
          Runtime.getRuntime().halt(1)
      }
    }
  }

  // High watermark do not need to be checkpointed only when under unit tests
  def shutdown(checkpointHW: Boolean = true) {
    info("Shutting down")
    replicaFetcherManager.shutdown()
    delayedFetchPurgatory.shutdown()
    delayedProducePurgatory.shutdown()
    if (checkpointHW)
      checkpointHighWatermarks()
    info("Shut down completely")
  }
}
