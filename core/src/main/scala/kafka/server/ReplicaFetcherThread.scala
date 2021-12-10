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

import java.net.SocketTimeoutException

import kafka.admin.AdminUtils
import kafka.cluster.BrokerEndPoint
import kafka.log.LogConfig
import kafka.message.ByteBufferMessageSet
import kafka.api.{KAFKA_0_10_0_IV0, KAFKA_0_9_0}
import kafka.common.{KafkaStorageException, TopicAndPartition}
import ReplicaFetcherThread._
import org.apache.kafka.clients.{ManualMetadataUpdater, NetworkClient, ClientRequest, ClientResponse}
import org.apache.kafka.common.network.{LoginType, Selectable, ChannelBuilders, NetworkReceive, Selector, Mode}
import org.apache.kafka.common.requests.{ListOffsetResponse, FetchResponse, RequestSend, AbstractRequest, ListOffsetRequest}
import org.apache.kafka.common.requests.{FetchRequest => JFetchRequest}
import org.apache.kafka.common.security.ssl.SslFactory
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{Errors, ApiKeys}
import org.apache.kafka.common.utils.Time

import scala.collection.{JavaConverters, Map, mutable}
import JavaConverters._

/**
 * 副本拉取线程
 * @param name
 * @param fetcherId
 * @param sourceBroker
 * @param brokerConfig
 * @param replicaMgr
 * @param metrics
 * @param time
 */
class ReplicaFetcherThread(name: String,
                           fetcherId: Int,
                           sourceBroker: BrokerEndPoint,
                           brokerConfig: KafkaConfig,
                           replicaMgr: ReplicaManager,
                           metrics: Metrics,
                           time: Time)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                sourceBroker = sourceBroker,
                                fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
                                isInterruptible = false) {
  //消息拉取请求
  type REQ = FetchRequest
  //拉取的数据
  type PD = PartitionData
  //拉取请求版本
  private val fetchRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_0_IV0) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
    else 0
  private val socketTimeout: Int = brokerConfig.replicaSocketTimeoutMs
  //当前副本id
  private val replicaId = brokerConfig.brokerId
  //副本拉取最大等待时间，默认500
  private val maxWait = brokerConfig.replicaFetchWaitMaxMs
  //副本拉取最小字节数，默认1
  private val minBytes = brokerConfig.replicaFetchMinBytes
  //副本拉取最大字节数，默认1024 * 1024
  private val fetchSize = brokerConfig.replicaFetchMaxBytes

  private def clientId = name

  private val sourceNode = new Node(sourceBroker.id, sourceBroker.host, sourceBroker.port)

  // we need to include both the broker id and the fetcher id 我们需要同时包含代理id和取数器id作为度量标记，
  // as the metrics tag to avoid metric name conflicts with 以避免度量名称与同一代理的多个取数器线程冲突
  // more than one fetcher thread to the same broker
  private val networkClient = {
    val channelBuilder = ChannelBuilders.create(
      brokerConfig.interBrokerSecurityProtocol,
      Mode.CLIENT,
      LoginType.SERVER,
      brokerConfig.values,
      brokerConfig.saslMechanismInterBrokerProtocol,
      brokerConfig.saslInterBrokerHandshakeRequestEnable
    )
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      brokerConfig.connectionsMaxIdleMs,
      metrics,
      time,
      "replica-fetcher",
      Map("broker-id" -> sourceBroker.id.toString, "fetcher-id" -> fetcherId.toString).asJava,
      false,
      channelBuilder
    )
    new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      clientId,
      1,
      0,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      brokerConfig.replicaSocketReceiveBufferBytes,
      brokerConfig.requestTimeoutMs,
      time
    )
  }

  override def shutdown(): Unit = {
    super.shutdown()
    networkClient.close()
  }

  /**
   * process fetched data
   * 处理获取的数据
   * @param topicAndPartition
   * @param fetchOffset
   * @param partitionData
   */
  def processPartitionData(topicAndPartition: TopicAndPartition, fetchOffset: Long, partitionData: PartitionData) {
    try {
      val TopicAndPartition(topic, partitionId) = topicAndPartition
      //根据主题和分区获取当前Broker上的副本
      val replica = replicaMgr.getReplica(topic, partitionId).get
      val messageSet = partitionData.toByteBufferMessageSet
      warnIfMessageOversized(messageSet, topicAndPartition)
      //拉取偏移和副本LEO的偏移不一致
      if (fetchOffset != replica.logEndOffset.messageOffset)
        throw new RuntimeException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(topicAndPartition, fetchOffset, replica.logEndOffset.messageOffset))
      if (logger.isTraceEnabled)
        trace("Follower %d has replica log end offset %d for partition %s. Received %d messages and leader hw %d"
          .format(replica.brokerId, replica.logEndOffset.messageOffset, topicAndPartition, messageSet.sizeInBytes, partitionData.highWatermark))
      //当前副本把拉取的数据追加至本地消息文件
      replica.log.get.append(messageSet, assignOffsets = false)
      if (logger.isTraceEnabled)
        trace("Follower %d has replica log end offset %d after appending %d bytes of messages for partition %s"
          .format(replica.brokerId, replica.logEndOffset.messageOffset, messageSet.sizeInBytes, topicAndPartition))
      //Follower的HW = Leader的HW和自己的LEO取小值
      val followerHighWatermark = replica.logEndOffset.messageOffset.min(partitionData.highWatermark)
      // for the follower replica, we do not need to keep 对于跟随者副本，
      // its segment base offset the physical position, 我们不需要将其日志段基础偏移保持在物理位置
      // these values will be computed upon making the leader 这些值将在生成Leader时计算
      replica.highWatermark = new LogOffsetMetadata(followerHighWatermark)//更新Follower的HW
      if (logger.isTraceEnabled)
        trace("Follower %d set replica high watermark for partition [%s,%d] to %s"
          .format(replica.brokerId, topic, partitionId, followerHighWatermark))
    } catch {
      case e: KafkaStorageException =>
        fatal(s"Disk error while replicating data for $topicAndPartition", e)
        Runtime.getRuntime.halt(1)
    }
  }

  def warnIfMessageOversized(messageSet: ByteBufferMessageSet, topicAndPartition: TopicAndPartition): Unit = {
    if (messageSet.sizeInBytes > 0 && messageSet.validBytes <= 0)
      error(s"Replication is failing due to a message that is greater than replica.fetch.max.bytes for partition $topicAndPartition. " +
        "This generally occurs when the max.message.bytes has been overridden to exceed this value and a suitably large " +
        "message has also been sent. To fix this problem increase replica.fetch.max.bytes in your broker config to be " +
        "equal or larger than your settings for max.message.bytes, both at a broker and topic level.")
  }

  /**
   * Handle a partition whose offset is out of range and return a new fetch offset.
   * 处理偏移量超出范围的分区并返回新的获取偏移量。
   */
  def handleOffsetOutOfRange(topicAndPartition: TopicAndPartition): Long = {
    //根据主题分区获取当前Broker的副本
    val replica = replicaMgr.getReplica(topicAndPartition.topic, topicAndPartition.partition).get

    /**
     * Unclean leader election: A follower goes down, in the meanwhile the leader keeps appending messages. The follower comes back up
     * and before it has completely caught up with the leader's logs, all replicas in the ISR go down. The follower is now uncleanly
     * elected as the new leader, and it starts appending messages from the client. The old leader comes back up, becomes a follower
     * and it may discover that the current leader's end offset is behind its own end offset.
     * 不洁领袖选举：一名追随者倒下，与此同时，领袖不断附加信息。跟随者返回，在它完全赶上领导者的日志之前，ISR中的所有副本都会下降。
     * 跟随者现在被不干净地选为新的领导者，并且它开始附加来自客户机的消息。旧Leader返回，成为跟随者，它可能会发现当前Leader的端点偏移位于其自身的端点偏移之后。
     * In such a case, truncate the current follower's log to the current leader's end offset and continue fetching.
     * 在这种情况下，将当前跟随者的日志截断为当前引导者的结束偏移量，然后继续获取
     * There is a potential for a mismatch between the logs of the two replicas here. We don't fix this mismatch as of now.
     * 此处两个副本的日志可能不匹配。到目前为止，我们尚未修复此不匹配
     */
    //向Leader副本发送请求，获取最新偏移量
    val leaderEndOffset: Long = earliestOrLatestOffset(topicAndPartition, ListOffsetRequest.LATEST_TIMESTAMP,
      brokerConfig.brokerId)
    //Leader副本的LEO小于当前副本的LEO
    if (leaderEndOffset < replica.logEndOffset.messageOffset) {
      // Prior to truncating the follower's log, ensure that doing so is not disallowed by the configuration for unclean leader election. 在截断跟随者的日志之前，请确保不干净领袖选举的配置不允许这样做。
      // This situation could only happen if the unclean election configuration for a topic changes while a replica is down. Otherwise, 只有在副本关闭时，主题的不干净选择配置发生更改时，才会发生这种情况。
      // we should never encounter this situation since a non-ISR leader cannot be elected if disallowed by the broker configuration. 否则，我们永远不会遇到这种情况，因为如果代理配置不允许，则无法选举非ISR领导人
      if (!LogConfig.fromProps(brokerConfig.originals, AdminUtils.fetchEntityConfig(replicaMgr.zkUtils,
        ConfigType.Topic, topicAndPartition.topic)).uncleanLeaderElectionEnable) {//"unclean.leader.election.enable"默认为true
        // Log a fatal error and shutdown the broker to ensure that data loss does not unexpectedly occur. 记录致命错误并关闭代理以确保数据不会意外丢失
        fatal("Exiting because log truncation is not allowed for topic %s,".format(topicAndPartition.topic) +
          " Current leader %d's latest offset %d is less than replica %d's latest offset %d"
          .format(sourceBroker.id, leaderEndOffset, brokerConfig.brokerId, replica.logEndOffset.messageOffset))
        System.exit(1)
      }

      warn("Replica %d for partition %s reset its fetch offset from %d to current leader %d's latest offset %d"
        .format(brokerConfig.brokerId, topicAndPartition, replica.logEndOffset.messageOffset, sourceBroker.id, leaderEndOffset))
      //当前Follower副本根据Leader副本的LEO截断日志
      replicaMgr.logManager.truncateTo(Map(topicAndPartition -> leaderEndOffset))
      leaderEndOffset
    } else {
      /**
       * If the leader's log end offset is greater than the follower's log end offset, there are two possibilities: 如果Leader的LEO大于Follower的LEO，则有两种可能性:
       * 1. The follower could have been down for a long time and when it starts up, its end offset could be smaller than the leader's 跟随器可能已关闭很长时间，当它启动时，其末端偏移量可能小于Leader的起始偏移量，
       * start offset because the leader has deleted old logs (log.logEndOffset < leaderStartOffset). 因为Leader已删除旧日志（log.logEndOffset<leaderstartfoset）。
       * 2. When unclean leader election occurs, it is possible that the old leader's high watermark is greater than 发生不干净的领导人选举时，旧Leader的HW可能大于新Leader的LEO。
       * the new leader's log end offset. So when the old leader truncates its offset to its high watermark and starts 因此，当旧Leader将其偏移量截断为其HW并开始从新Leader获取时，将抛出OffsetAutoFrangeException。
       * to fetch from the new leader, an OffsetOutOfRangeException will be thrown. After that some more messages are 之后，会向新Leader发送更多的信息。
       * produced to the new leader. While the old leader is trying to handle the OffsetOutOfRangeException and query 当旧Leader尝试处理OffsetAutoFrangeException并查询新Leader的LEO时，
       * the log end offset of the new leader, the new leader's log end offset becomes higher than the follower's log end offset. 新Leader的日志结束偏移将高于Follower的日志结束偏移。
       *
       * In the first case, the follower's current log end offset is smaller than the leader's log start offset. So the 在第一种情况下，跟随者的当前日志结束偏移小于Leader的日志开始偏移。
       * follower should truncate all its logs, roll out a new segment and start to fetch from the current leader's log 因此，跟随者应该截断其所有日志，展开一个新的段，并开始从当前Leader的日志开始偏移量提取
       * start offset.
       * In the second case, the follower should just keep the current log segments and retry the fetch. In the second 在第二种情况下，跟随者应该只保留当前日志段，然后重试获取。
       * case, there will be some inconsistency of data between old and new leader. We are not solving it here. 在第二种情况下，新旧Leader之间的数据会有一些不一致。我们不是在这里解决问题。
       * If users want to have strong consistency guarantees, appropriate configurations needs to be set for both 如果用户希望有强大的一致性保证，则需要为代理和生产者设置适当的配置
       * brokers and producers.
       *
       * Putting the two cases together, the follower should fetch from the higher one of its replica log end offset
       * and the current leader's log start offset. 将这两种情况放在一起，跟随者应该从其副本LEO和当前Leader日志开始偏移量中较高的一个获取。
       *
       */
      //获取Leader副本的开始偏移量
      val leaderStartOffset: Long = earliestOrLatestOffset(topicAndPartition, ListOffsetRequest.EARLIEST_TIMESTAMP,
        brokerConfig.brokerId)
      warn("Replica %d for partition %s reset its fetch offset from %d to current leader %d's start offset %d"
        .format(brokerConfig.brokerId, topicAndPartition, replica.logEndOffset.messageOffset, sourceBroker.id, leaderStartOffset))
      //当前副本LEO和Leader的开始偏移取大者
      val offsetToFetch = Math.max(leaderStartOffset, replica.logEndOffset.messageOffset)
      // Only truncate log when current leader's log start offset is greater than follower's log end offset. 仅当当前Leader的日志开始偏移量大于跟随者的LEO时，才截断日志
      if (leaderStartOffset > replica.logEndOffset.messageOffset)
        replicaMgr.logManager.truncateFullyAndStartAt(topicAndPartition, leaderStartOffset)//删除分区中的所有数据，并在新的偏移量处启动日志
      offsetToFetch
    }
  }

  // any logic for partitions whose leader has changed Leader已更改的分区的任何逻辑
  def handlePartitionsWithErrors(partitions: Iterable[TopicAndPartition]) {
    delayPartitions(partitions, brokerConfig.replicaFetchBackoffMs.toLong)
  }

  /**
   * 拉取操作
   * @param fetchRequest 拉取请求
   * @return
   */
  protected def fetch(fetchRequest: FetchRequest): Map[TopicAndPartition, PartitionData] = {
    val clientResponse = sendRequest(ApiKeys.FETCH, Some(fetchRequestVersion), fetchRequest.underlying)
    new FetchResponse(clientResponse.responseBody).responseData.asScala.map { case (key, value) =>
      TopicAndPartition(key.topic, key.partition) -> new PartitionData(value)
    }
  }

  /**
   * 发送拉取请求
   * @param apiKey
   * @param apiVersion
   * @param request
   * @return
   */
  private def sendRequest(apiKey: ApiKeys, apiVersion: Option[Short], request: AbstractRequest): ClientResponse = {
    import kafka.utils.NetworkClientBlockingOps._
    val header = apiVersion.fold(networkClient.nextRequestHeader(apiKey))(networkClient.nextRequestHeader(apiKey, _))
    try {
      if (!networkClient.blockingReady(sourceNode, socketTimeout)(time))
        throw new SocketTimeoutException(s"Failed to connect within $socketTimeout ms")
      else {
        val send = new RequestSend(sourceBroker.id.toString, header, request.toStruct)
        val clientRequest = new ClientRequest(time.milliseconds(), true, send, null)
        networkClient.blockingSendAndReceive(clientRequest)(time)
      }
    }
    catch {
      case e: Throwable =>
        networkClient.close(sourceBroker.id.toString)
        throw e
    }

  }

  /**
   * 向Leader副本发送请求，获取最早或最晚偏移量
   * @param topicAndPartition
   * @param earliestOrLatest
   * @param consumerId
   * @return
   */
  private def earliestOrLatestOffset(topicAndPartition: TopicAndPartition, earliestOrLatest: Long, consumerId: Int): Long = {
    val topicPartition = new TopicPartition(topicAndPartition.topic, topicAndPartition.partition)
    val partitions = Map(
      topicPartition -> new ListOffsetRequest.PartitionData(earliestOrLatest, 1)
    )
    val request = new ListOffsetRequest(consumerId, partitions.asJava)
    val clientResponse = sendRequest(ApiKeys.LIST_OFFSETS, None, request)
    val response = new ListOffsetResponse(clientResponse.responseBody)
    val partitionData = response.responseData.get(topicPartition)
    Errors.forCode(partitionData.errorCode) match {
      case Errors.NONE => partitionData.offsets.asScala.head
      case errorCode => throw errorCode.exception
    }
  }

  /**
   * 构建拉取请求
   * @param partitionMap 分区Map
   * @return
   */
  protected def buildFetchRequest(partitionMap: Map[TopicAndPartition, PartitionFetchState]): FetchRequest = {
    val requestMap = mutable.Map.empty[TopicPartition, JFetchRequest.PartitionData]

    partitionMap.foreach { case ((TopicAndPartition(topic, partition), partitionFetchState)) =>
      if (partitionFetchState.isActive) {
        //fetchSize拉取的数据量是多大默认1m
        requestMap(new TopicPartition(topic, partition)) = new JFetchRequest.PartitionData(partitionFetchState.offset, fetchSize)
      }
    }

    new FetchRequest(new JFetchRequest(replicaId, maxWait, minBytes, requestMap.asJava))
  }

}

object ReplicaFetcherThread {

  private[server] class FetchRequest(val underlying: JFetchRequest) extends AbstractFetcherThread.FetchRequest {
    def isEmpty: Boolean = underlying.fetchData.isEmpty
    def offset(topicAndPartition: TopicAndPartition): Long =
      underlying.fetchData.asScala(new TopicPartition(topicAndPartition.topic, topicAndPartition.partition)).offset
  }

  private[server] class PartitionData(val underlying: FetchResponse.PartitionData) extends AbstractFetcherThread.PartitionData {

    def errorCode: Short = underlying.errorCode

    def toByteBufferMessageSet: ByteBufferMessageSet = new ByteBufferMessageSet(underlying.recordSet)

    def highWatermark: Long = underlying.highWatermark

    def exception: Option[Throwable] = Errors.forCode(errorCode) match {
      case Errors.NONE => None
      case e => Some(e.exception)
    }

  }

}
