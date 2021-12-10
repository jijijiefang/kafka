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

import kafka.common._
import kafka.utils._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.log.LogConfig
import kafka.server._
import kafka.metrics.KafkaMetricsGroup
import kafka.controller.KafkaController
import kafka.message.ByteBufferMessageSet
import java.io.IOException
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.kafka.common.errors.{NotEnoughReplicasException, NotLeaderForPartitionException}
import org.apache.kafka.common.protocol.Errors

import scala.collection.JavaConverters._
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.requests.PartitionState

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 * 表示主题分区的数据结构。领导维护AR、ISR、CUR、RAR
 */
class Partition(val topic: String,
                val partitionId: Int,
                time: Time,
                replicaManager: ReplicaManager) extends Logging with KafkaMetricsGroup {
  private val localBrokerId = replicaManager.config.brokerId
  private val logManager = replicaManager.logManager
  private val zkUtils = replicaManager.zkUtils
  private val assignedReplicaMap = new Pool[Int, Replica]
  // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock()
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  @volatile var leaderReplicaIdOpt: Option[Int] = None
  @volatile var inSyncReplicas: Set[Replica] = Set.empty[Replica]

  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  this.logIdent = "Partition [%s,%d] on broker %d: ".format(topic, partitionId, localBrokerId)

  private def isReplicaLocal(replicaId: Int) : Boolean = (replicaId == localBrokerId)
  val tags = Map("topic" -> topic, "partition" -> partitionId.toString)

  newGauge("UnderReplicated",
    new Gauge[Int] {
      def value = {
        if (isUnderReplicated) 1 else 0
      }
    },
    tags
  )

  def isUnderReplicated(): Boolean = {
    leaderReplicaIfLocal() match {
      case Some(_) =>
        inSyncReplicas.size < assignedReplicas.size
      case None =>
        false
    }
  }

  def getOrCreateReplica(replicaId: Int = localBrokerId): Replica = {
    val replicaOpt = getReplica(replicaId)
    replicaOpt match {
      case Some(replica) => replica
      case None =>
        if (isReplicaLocal(replicaId)) {
          val config = LogConfig.fromProps(logManager.defaultConfig.originals,
                                           AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, topic))
          val log = logManager.createLog(TopicAndPartition(topic, partitionId), config)
          val checkpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParentFile.getAbsolutePath)
          val offsetMap = checkpoint.read
          if (!offsetMap.contains(TopicAndPartition(topic, partitionId)))
            info("No checkpointed highwatermark is found for partition [%s,%d]".format(topic, partitionId))
          val offset = offsetMap.getOrElse(TopicAndPartition(topic, partitionId), 0L).min(log.logEndOffset)
          val localReplica = new Replica(replicaId, this, time, offset, Some(log))
          addReplicaIfNotExists(localReplica)
        } else {
          val remoteReplica = new Replica(replicaId, this, time)
          addReplicaIfNotExists(remoteReplica)
        }
        getReplica(replicaId).get
    }
  }

  def getReplica(replicaId: Int = localBrokerId): Option[Replica] = {
    val replica = assignedReplicaMap.get(replicaId)
    if (replica == null)
      None
    else
      Some(replica)
  }

  def leaderReplicaIfLocal(): Option[Replica] = {
    leaderReplicaIdOpt match {
      case Some(leaderReplicaId) =>
        if (leaderReplicaId == localBrokerId)
          getReplica(localBrokerId)
        else
          None
      case None => None
    }
  }

  def addReplicaIfNotExists(replica: Replica) = {
    assignedReplicaMap.putIfNotExists(replica.brokerId, replica)
  }

  def assignedReplicas(): Set[Replica] = {
    assignedReplicaMap.values.toSet
  }

  def removeReplica(replicaId: Int) {
    assignedReplicaMap.remove(replicaId)
  }

  def delete() {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) {
      assignedReplicaMap.clear()
      inSyncReplicas = Set.empty[Replica]
      leaderReplicaIdOpt = None
      try {
        logManager.deleteLog(TopicAndPartition(topic, partitionId))
        removePartitionMetrics()
      } catch {
        case e: IOException =>
          fatal("Error deleting the log for partition [%s,%d]".format(topic, partitionId), e)
          Runtime.getRuntime().halt(1)
      }
    }
  }

  def getLeaderEpoch(): Int = {
    return this.leaderEpoch
  }

  /**
   * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset
   * from the time when this broker was the leader last time) and setting the new leader and ISR.
   * If the leader replica id does not change, return false to indicate the replica manager.
   * 通过重置远程副本的LogEndOffset（从上次此代理是Leader时起可能存在旧的LogEndOffset）并设置新的Leader和ISR，使本地副本成为Leader。
   * 如果Leader副本id没有更改，则返回false以指示副本管理器
   */
  def makeLeader(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr 记录做出领导决策的控制者的时代。
      // to maintain the decision maker controller's epoch in the zookeeper path 这在更新isr以在zookeeper路径中维护决策者控制器的历元时非常有用
      controllerEpoch = partitionStateInfo.controllerEpoch
      // add replicas that are new 添加新的复制副本
      allReplicas.foreach(replica => getOrCreateReplica(replica))
      //新是ISR集合
      val newInSyncReplicas = partitionStateInfo.isr.asScala.map(r => getOrCreateReplica(r)).toSet
      // remove assigned replicas that have been removed by the controller 删除AR集合中已经被控制器删除的
      (assignedReplicas().map(_.brokerId) -- allReplicas).foreach(removeReplica(_))
      inSyncReplicas = newInSyncReplicas
      leaderEpoch = partitionStateInfo.leaderEpoch
      zkVersion = partitionStateInfo.zkVersion
      val isNewLeader =
        if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == localBrokerId) {
          false
        } else {
          leaderReplicaIdOpt = Some(localBrokerId)
          true
        }
      val leaderReplica = getReplica().get
      // we may need to increment high watermark since ISR could be down to 1 我们可能需要增加HW，因为ISR可能降至1
      if (isNewLeader) {
        // construct the high watermark metadata for the new leader replica 为新的领导者副本构建HW元数据
        leaderReplica.convertHWToLocalOffsetMetadata()
        // reset log end offset for remote replicas 重置远程副本的日志结束偏移量
        assignedReplicas.filter(_.brokerId != localBrokerId).foreach(_.updateLogReadResult(LogReadResult.UnknownLogReadResult))
      }
      //可能会更新HW
      (maybeIncrementLeaderHW(leaderReplica), isNewLeader)
    }
    // some delayed operations may be unblocked after HW changed 更新HW后，某些延迟的操作可能会被解除阻止
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
    isNewLeader
  }

  /**
   *  Make the local replica the follower by setting the new leader and ISR to empty
   *  If the leader replica id does not change, return false to indicate the replica manager
   *  通过将新的Leader和ISR设置为空，使本地副本成为跟随者。如果先导Leaderid没有更改，则返回false以指示副本管理器
   */
  def makeFollower(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      val newLeaderBrokerId: Int = partitionStateInfo.leader
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr 记录做出领导决策的控制者的时代。
      // to maintain the decision maker controller's epoch in the zookeeper path 这在更新isr以在zookeeper路径中维护决策者控制器的历元时非常有用
      controllerEpoch = partitionStateInfo.controllerEpoch
      // add replicas that are new 添加新的副本
      allReplicas.foreach(r => getOrCreateReplica(r))
      // remove assigned replicas that have been removed by the controller 删除AR中已被控制器删除的副本
      (assignedReplicas().map(_.brokerId) -- allReplicas).foreach(removeReplica(_))
      inSyncReplicas = Set.empty[Replica]
      leaderEpoch = partitionStateInfo.leaderEpoch
      zkVersion = partitionStateInfo.zkVersion

      if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == newLeaderBrokerId) {
        false
      }
      else {
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  /**
   * Update the log end offset of a certain replica of this partition
   * 更新分区副本的LEO
   */
  def updateReplicaLogReadResult(replicaId: Int, logReadResult: LogReadResult) {

    getReplica(replicaId) match {
      case Some(replica) =>
        //更新这个Follower的LEO
        replica.updateLogReadResult(logReadResult)
        // check if we need to expand ISR to include this replica 检查是否需要扩展ISR以包括此复制副本
        // if it is not in the ISR yet 如果它尚未包含在ISR中
        maybeExpandIsr(replicaId)

        debug("Recorded replica %d log end offset (LEO) position %d for partition %s."
          .format(replicaId,
                  logReadResult.info.fetchOffsetMetadata.messageOffset,
                  TopicAndPartition(topic, partitionId)))
      case None =>
        throw new NotAssignedReplicaException(("Leader %d failed to record follower %d's position %d since the replica" +
          " is not recognized to be one of the assigned replicas %s for partition %s.")
          .format(localBrokerId,
                  replicaId,
                  logReadResult.info.fetchOffsetMetadata.messageOffset,
                  assignedReplicas().map(_.brokerId).mkString(","),
                  TopicAndPartition(topic, partitionId)))
    }
  }

  /**
   * Check and maybe expand the ISR of the partition.
   * 检查并可能扩展分区的ISR。
   * This function can be triggered when a replica's LEO has incremented
   * 当副本的LEO增加时，可以触发此功能
   */
  def maybeExpandIsr(replicaId: Int) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      // check if this replica needs to be added to the ISR 检查是否需要将此复制副本添加到ISR
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          //Follower副本
          val replica = getReplica(replicaId).get
          //Leader的HW
          val leaderHW = leaderReplica.highWatermark
          //如果ISR不包含此副本，AR包含此副本且此副本的LEO大于Leader的HW
          if(!inSyncReplicas.contains(replica) &&
             assignedReplicas.map(_.brokerId).contains(replicaId) &&
                  replica.logEndOffset.offsetDiff(leaderHW) >= 0) {
            val newInSyncReplicas = inSyncReplicas + replica
            info("Expanding ISR for partition [%s,%d] from %s to %s"
                         .format(topic, partitionId, inSyncReplicas.map(_.brokerId).mkString(","),
                                 newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in ZK and cache 更新ISR列表
            updateIsr(newInSyncReplicas)
            replicaManager.isrExpandRate.mark()
          }

          // check if the HW of the partition can now be incremented 检查分区的HW现在是否可以增加，因为复制副本现在可能在ISR中，并且其LEO刚刚增加
          // since the replica maybe now be in the ISR and its LEO has just incremented
          //更新HW
          maybeIncrementLeaderHW(leaderReplica)

        case None => false // nothing to do if no longer leader 如果不再是领导者，就没什么可做的了
      }
    }

    // some delayed operations may be unblocked after HW changed HW变化后某些延迟操作可以停止挂起
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
  }

  /*
   * Note that this method will only be called if requiredAcks = -1
   * and we are waiting for all replicas in ISR to be fully caught up to
   * the (local) leader's offset corresponding to this produce request
   * before we acknowledge the produce request.
   * 请注意，只有当requiredAcks=-1并且我们正在等待ISR中的所有副本完全赶上与此生产请求相对应的（本地）领导者偏移量时，才会调用此方法，然后我们才确认生产请求
   */
  def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Short) = {
    leaderReplicaIfLocal() match {
      case Some(leaderReplica) =>
        // keep the current immutable replica list reference ISR列表
        val curInSyncReplicas = inSyncReplicas
        //遍历ISR列表
        val numAcks = curInSyncReplicas.count(r => {
          //如果副本不是Leader副本
          if (!r.isLocal) {
            //检查此副本的LEO是否大于所需偏移量返回true，否则返回false
            if (r.logEndOffset.messageOffset >= requiredOffset) {
              trace("Replica %d of %s-%d received offset %d".format(r.brokerId, topic, partitionId, requiredOffset))
              true
            }
            else
              false
          //当前的Leader副本直接返回true
          } else
            true /* also count the local (leader) replica 还要计算本地（Leader）副本的数量*/
        })

        trace("%d acks satisfied for %s-%d with acks = -1".format(numAcks, topic, partitionId))
        //最小同步副本数
        val minIsr = leaderReplica.log.get.config.minInSyncReplicas
        //如果Leader副本的HW大于等于所需偏移量
        if (leaderReplica.highWatermark.messageOffset >= requiredOffset ) {
          /*
          * The topic may be configured not to accept messages if there are not enough replicas in ISR
          * in this scenario the request was already appended locally and then added to the purgatory before the ISR was shrunk
          * 如果ISR中没有足够的副本，则可以将主题配置为不接受消息。在这种情况下，请求已在本地附加，然后在ISR收缩之前添加到炼狱中
          */
          //如果小于等于当前ISR列表数量
          if (minIsr <= curInSyncReplicas.size) {
            //设置为true，当前追加已经满足条件
            (true, Errors.NONE.code)
          } else {
            //设置错误码，没有足够的副本
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND.code)
          }
        } else {
          //如果Leader副本的HW小于所需偏移量，发生本地错误
          //设置为false，当前追加已经不满足条件
          (false, Errors.NONE.code)
        }
      case None =>
        (false, Errors.NOT_LEADER_FOR_PARTITION.code)
    }
  }

  /**
   * Check and maybe increment the high watermark of the partition;
   * this function can be triggered when
   * 检查并可能增加分区的高水位
   * 1. Partition ISR changed ISR列表变更
   * 2. Any replica's LEO changed 某个副本的LEO变更
   * 如果HW递增，则返回true，否则返回false
   * Returns true if the HW was incremented, and false otherwise.
   * Note There is no need to acquire the leaderIsrUpdate lock here
   * since all callers of this private API acquire that lock
   */
  private def maybeIncrementLeaderHW(leaderReplica: Replica): Boolean = {
    //获取ISR中所有副本的LEO
    val allLogEndOffsets = inSyncReplicas.map(_.logEndOffset)
    //取ISR列表中LEO的最小值作为新的HW
    val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering)
    //Leader分区的HW
    val oldHighWatermark = leaderReplica.highWatermark
    //如果旧HW小于新HW或旧HW在旧文件段上，新HW在新文件段上
    if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset || oldHighWatermark.onOlderSegment(newHighWatermark)) {
      //更新HW
      leaderReplica.highWatermark = newHighWatermark
      debug("High watermark for partition [%s,%d] updated to %s".format(topic, partitionId, newHighWatermark))
      true
    } else {
      debug("Skipping update high watermark since Old hw %s is larger than new hw %s for partition [%s,%d]. All leo's are %s"
        .format(oldHighWatermark, newHighWatermark, topic, partitionId, allLogEndOffsets.mkString(",")))
      false
    }
  }

  /**
   * Try to complete any pending requests. This should be called without holding the leaderIsrUpdateLock.
   * 尝试完成所有阻塞的请求
   */
  private def tryCompleteDelayedRequests() {
    val requestKey = new TopicPartitionOperationKey(this.topic, this.partitionId)
    replicaManager.tryCompleteDelayedFetch(requestKey)
    replicaManager.tryCompleteDelayedProduce(requestKey)
  }

  /**
   * 可能收缩ISR
   * @param replicaMaxLagTimeMs ISR列表滞后时间毫秒
   */
  def maybeShrinkIsr(replicaMaxLagTimeMs: Long) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          //OSR副本集合
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs)
          if(outOfSyncReplicas.size > 0) {
            //新的ISR副本集合
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas
            assert(newInSyncReplicas.size > 0)
            info("Shrinking ISR for partition [%s,%d] from %s to %s".format(topic, partitionId,
              inSyncReplicas.map(_.brokerId).mkString(","), newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in zk and in cache 在zk和缓存中更新ISR
            updateIsr(newInSyncReplicas)
            // we may need to increment high watermark since ISR could be down to 1 我们可能需要增加高水位标记，因为ISR可能降至1

            replicaManager.isrShrinkRate.mark()
            //检查并可能增加分区的高水位
            maybeIncrementLeaderHW(leaderReplica)
          } else {
            false
          }

        case None => false // do nothing if no longer leader 如果不再是领导者，什么也不要做
      }
    }

    // some delayed operations may be unblocked after HW changed HW变化后某些延迟操作会解除阻塞
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
  }

  /**
   * 获取OSR副本集合
   * @param leaderReplica Leader副本
   * @param maxLagMs 最大滞后毫秒数
   * @return
   */
  def getOutOfSyncReplicas(leaderReplica: Replica, maxLagMs: Long): Set[Replica] = {
    /**
     * there are two cases that will be handled here - 这里将处理两个案例 -
     * 1. Stuck followers: If the leo of the replica hasn't been updated for maxLagMs ms, 被卡住的跟随者：如果副本的LEO没有为maxLagMs更新，则跟随者被卡住，应该从ISR中删除
     *                     the follower is stuck and should be removed from the ISR
     * 2. Slow followers: If the replica has not read up to the leo within the last maxLagMs ms, 速跟随器：如果复制副本在最后一个maxLagMs内没有读取到leo，则跟随器处于滞后状态，应将其从ISR中删除。
     *                    then the follower is lagging and should be removed from the ISR
     * Both these cases are handled by checking the lastCaughtUpTimeMs which represents
     * the last time when the replica was fully caught up. If either of the above conditions
     * is violated, that replica is considered to be out of sync
     * 这两种情况都是通过检查lastCaughtUpTimeMs来处理的，lastCaughtUpTimeMs表示复制副本最后一次完全赶上的时间。如果违反上述任一条件，则该复制副本将被视为不同步
     **/
    //Leader副本的LEO
    val leaderLogEndOffset = leaderReplica.logEndOffset
    //ISR列表排除掉Leader副本
    val candidateReplicas = inSyncReplicas - leaderReplica
    //筛选出大于滞后时间的滞后副本集合
    val laggingReplicas = candidateReplicas.filter(r => (time.milliseconds - r.lastCaughtUpTimeMs) > maxLagMs)
    if(laggingReplicas.size > 0)
      debug("Lagging replicas for partition %s are %s".format(TopicAndPartition(topic, partitionId), laggingReplicas.map(_.brokerId).mkString(",")))

    laggingReplicas
  }

  /**
   * 追加消息至Leader
   * @param messages
   * @param requiredAcks
   * @return
   */
  def appendMessagesToLeader(messages: ByteBufferMessageSet, requiredAcks: Int = 0) = {
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      val leaderReplicaOpt = leaderReplicaIfLocal()
      leaderReplicaOpt match {
        //判断当前Partition是否是Leader
        case Some(leaderReplica) =>
          val log = leaderReplica.log.get
          val minIsr = log.config.minInSyncReplicas
          //ISR列表数量
          val inSyncSize = inSyncReplicas.size

          // Avoid writing to leader if there are not enough insync replicas to make it safe 如果没有足够的同步副本来确保安全，请避免写入领导者
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException("Number of insync replicas for partition [%s,%d] is [%d], below required minimum [%d]"
              .format(topic, partitionId, inSyncSize, minIsr))
          }
          //消息追加至文件
          val info = log.append(messages, assignOffsets = true)
          // probably unblock some follower fetch requests since log end offset has been updated 由于日志结束偏移量已更新，可能会取消阻止某些跟随者获取请求
          replicaManager.tryCompleteDelayedFetch(new TopicPartitionOperationKey(this.topic, this.partitionId))
          // we may need to increment high watermark since ISR could be down to 1 我们可能需要增加高水位，因为ISR可能降至1
          (info, maybeIncrementLeaderHW(leaderReplica))

        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
            .format(topic, partitionId, localBrokerId))
      }
    }

    // some delayed operations may be unblocked after HW changed HW变化后某些延迟操作会解除阻塞
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()

    info
  }

  /**
   * 更新ISR列表
   * @param newIsr
   */
  private def updateIsr(newIsr: Set[Replica]) {
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(r => r.brokerId).toList, zkVersion)
    val (updateSucceeded,newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partitionId,
      newLeaderAndIsr, controllerEpoch, zkVersion)

    if(updateSucceeded) {
      replicaManager.recordIsrChange(new TopicAndPartition(topic, partitionId))
      inSyncReplicas = newIsr
      zkVersion = newVersion
      trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.mkString(","), zkVersion))
    } else {
      info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
    }
  }

  /**
   * remove deleted log metrics
   */
  private def removePartitionMetrics() {
    removeMetric("UnderReplicated", tags)
  }

  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[Partition]))
      return false
    val other = that.asInstanceOf[Partition]
    if(topic.equals(other.topic) && partitionId == other.partitionId)
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17*partitionId
  }

  override def toString(): String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; AssignedReplicas: " + assignedReplicaMap.keys.mkString(","))
    partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
    partitionString.toString()
  }
}
