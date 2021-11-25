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
package kafka.controller

import collection._
import collection.JavaConversions._
import java.util.concurrent.atomic.AtomicBoolean
import kafka.common.{TopicAndPartition, StateChangeFailedException}
import kafka.utils.{ZkUtils, ReplicationUtils, Logging}
import org.I0Itec.zkclient.IZkChildListener
import org.apache.log4j.Logger
import kafka.controller.Callbacks._
import kafka.utils.CoreUtils._

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and 此类表示副本的状态机。它定义了复制副本可以处于的状态，以及将复制副本移动到另一个合法状态的转换
 * transitions to move the replica to another legal state. The different states that a replica can be in are - 复制副本可以处于的不同状态为
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 *                        replica can only get become follower state change request.  Valid previous
 *                        state is NonExistentReplica 控制器可以在分区重新分配期间创建新副本。在此状态下，复制副本只能获得状态更改请求。有效的前一状态不存在副本
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 *                        state. In this state, it can get either become leader or become follower state change requests.
 *                        Valid previous state are NewReplica, OnlineReplica or OfflineReplica 一旦一个复制副本启动并为其分区分配了一部分复制副本，它就处于这种状态。在这种状态下，它可以获得“成为领导者”或“成为跟随者”状态更改请求。以前的有效状态为NewReplica、OnlineReplica或OfflineReplica
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 *                        is down. Valid previous state are NewReplica, OnlineReplica 如果副本死亡，它将移动到此状态。当承载复制副本的代理关闭时，会发生这种情况。以前的有效状态为NewReplica，OnlineReplica
 * 4. ReplicaDeletionStarted: If replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica 如果副本删除开始，它将移动到此状态。以前的有效状态为脱机副本
 * 5. ReplicaDeletionSuccessful: If replica responds with no error code in response to a delete replica request, it is
 *                        moved to this state. Valid previous state is ReplicaDeletionStarted 如果复制副本响应删除复制副本请求时没有错误代码，则将其移动到此状态。以前的有效状态为ReplicateDeletionStarted
 * 6. ReplicaDeletionIneligible: If replica deletion fails, it is moved to this state. Valid previous state is ReplicaDeletionStarted 如果副本删除失败，它将移动到此状态。以前的有效状态为ReplicateDeletionStarted
 * 7. NonExistentReplica: If a replica is deleted successfully, it is moved to this state. Valid previous state is
 *                        ReplicaDeletionSuccessful 如果成功删除复制副本，则将其移动到此状态。有效的先前状态为ReplicateDeletionSuccessful
 */
class ReplicaStateMachine(controller: KafkaController) extends Logging {
  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId
  private val zkUtils = controllerContext.zkUtils
  private val replicaState: mutable.Map[PartitionAndReplica, ReplicaState] = mutable.Map.empty
  private val brokerChangeListener = new BrokerChangeListener()
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(controller)
  private val hasStarted = new AtomicBoolean(false)
  private val stateChangeLogger = KafkaController.stateChangeLogger

  this.logIdent = "[Replica state machine on controller " + controller.config.brokerId + "]: "


  /**
   * Invoked on successful controller election. First registers a broker change listener since that triggers all
   * state transitions for replicas. Initializes the state of replicas for all partitions by reading from zookeeper.
   * Then triggers the OnlineReplica state change for all replicas.
   * 在控制器选举成功后调用。首先注册Broker更改监听器，因为它会触发副本的所有状态转换。通过读取zookeeper来初始化所有分区的副本状态
   * 然后触发所有副本更改为OnlineReplica状态
   */
  def startup() {
    // initialize replica state 初始化副本状态
    initializeReplicaState()
    // set started flag 设置已启动标识
    hasStarted.set(true)
    // move all Online replicas to Online 修改所有在线的副本状态为“在线副本”
    handleStateChanges(controllerContext.allLiveReplicas(), OnlineReplica)

    info("Started replica state machine with initial state -> " + replicaState.toString())
  }

  // register ZK listeners of the replica state machine 注册副本状态机的ZK监听器
  def registerListeners() {
    // register broker change listener 注册Broker变化监听器
    registerBrokerChangeListener()
  }

  // de-register ZK listeners of the replica state machine 取消注册副本状态机的ZK监听器
  def deregisterListeners() {
    // de-register broker change listener 取消注册Broker变化监听器
    deregisterBrokerChangeListener()
  }

  /**
   * Invoked on controller shutdown. 在控制器关闭时调用
   */
  def shutdown() {
    // reset started flag 重置已启动标记
    hasStarted.set(false)
    // reset replica state 重置副本状态
    replicaState.clear()
    // de-register all ZK listeners 取消注册所有ZK监听器
    deregisterListeners()

    info("Stopped replica state machine")
  }

  /**
   * This API is invoked by the broker change controller callbacks and the startup API of the state machine 此API由代理更改控制器回调和状态机的启动API调用
   * @param replicas     The list of replicas (brokers) that need to be transitioned to the target state 需要转换到目标状态的副本（代理）列表
   * @param targetState  The state that the replicas should be moved to 目标状态
   * The controller's allLeaders cache should have been updated before this 在此之前控制器的所有Leader缓存已经被设置
   */
  def handleStateChanges(replicas: Set[PartitionAndReplica], targetState: ReplicaState,
                         callbacks: Callbacks = (new CallbackBuilder).build) {
    if(replicas.size > 0) {
      info("Invoking state change to %s for replicas %s".format(targetState, replicas.mkString(",")))
      try {
        brokerRequestBatch.newBatch()
        replicas.foreach(r => handleStateChange(r, targetState, callbacks))
        brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
      }catch {
        case e: Throwable => error("Error while moving some replicas to %s state".format(targetState), e)
      }
    }
  }

  /**
   * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentReplica --> NewReplica 不存在副本 --> 新副本
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker 将当前leader和isr的LeaderAndIsr请求发送到新副本，并将此分区的UpdateMetadata请求发送到每个live broker
   *
   * NewReplica -> OnlineReplica 新副本 --> 在线副本
   * --add the new replica to the assigned replica list if needed 如果需要，将新副本添加到AR列表中
   *
   * OnlineReplica,OfflineReplica -> OnlineReplica 在线副本、不在线副本 -> 在线副本
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker 将当前leader和isr的LeaderAndIsr请求发送到新副本，并将此分区的UpdateMetadata请求发送到每个live broker
   *
   * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionIneligible -> OfflineReplica 新副本、在线副本、不在线副本、副本删除异常 -> 不在线副本
   * --send StopReplicaRequest to the replica (w/o deletion) 想副本发送StopReplicaRequest
   * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
   *   UpdateMetadata request for the partition to every live broker. 从isr中删除此复制副本，并将LeaderAndIsr请求（使用新的isr）发送给leader副本，并将分区的UpdateMetadata请求发送给每个live broker。
   *
   * OfflineReplica -> ReplicaDeletionStarted 不在线副本 -> 副本删除已开始
   * --send StopReplicaRequest to the replica (with deletion) 向复制副本发送StopReplicaRequest（删除）
   *
   * ReplicaDeletionStarted -> ReplicaDeletionSuccessful 副本删除已开始 -> 副本删除成功
   * -- mark the state of the replica in the state machine 在状态机中标记副本的状态
   *
   * ReplicaDeletionStarted -> ReplicaDeletionIneligible 副本删除已开始 -> 副本删除异常
   * -- mark the state of the replica in the state machine 在状态机中标记副本的状态
   *
   * ReplicaDeletionSuccessful -> NonExistentReplica 副本删除成功 -> 不存在副本
   * -- remove the replica from the in memory partition replica assignment cache 从内存分区副本分配缓存中删除副本


   * @param partitionAndReplica The replica for which the state transition is invoked
   * @param targetState The end state that the replica should be moved to
   */
  def handleStateChange(partitionAndReplica: PartitionAndReplica, targetState: ReplicaState,
                        callbacks: Callbacks) {
    val topic = partitionAndReplica.topic
    val partition = partitionAndReplica.partition
    val replicaId = partitionAndReplica.replica
    val topicAndPartition = TopicAndPartition(topic, partition)
    if (!hasStarted.get)
      throw new StateChangeFailedException(("Controller %d epoch %d initiated state change of replica %d for partition %s " +
                                            "to %s failed because replica state machine has not started")
                                              .format(controllerId, controller.epoch, replicaId, topicAndPartition, targetState))
    val currState = replicaState.getOrElseUpdate(partitionAndReplica, NonExistentReplica)
    try {
      //获取次主题分区分配的副本列表
      val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
      //目标状态
      targetState match {
        case NewReplica =>
          assertValidPreviousStates(partitionAndReplica, List(NonExistentReplica), targetState)
          // start replica as a follower to the current leader for its partition 将副本作为其分区的当前Leader的跟随者启动
          val leaderIsrAndControllerEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
          leaderIsrAndControllerEpochOpt match {
            //存在Leader副本
            case Some(leaderIsrAndControllerEpoch) =>
              //当前副本是Leader副本
              if(leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId)
                throw new StateChangeFailedException("Replica %d for partition %s cannot be moved to NewReplica"
                  .format(replicaId, topicAndPartition) + "state as it is being requested to become leader") //当前副本是Leader副本不能转换为NewReplica状态

              brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId),
                                                                  topic, partition, leaderIsrAndControllerEpoch,
                                                                  replicaAssignment)
            case None => // new leader request will be sent to this replica when one gets elected 当选后，新的领导请求将发送到此副本
          }
          //修改状态为NewReplica
          replicaState.put(partitionAndReplica, NewReplica)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                                    .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState,
                                            targetState))
        case ReplicaDeletionStarted =>
          assertValidPreviousStates(partitionAndReplica, List(OfflineReplica), targetState)
          //设置状态为ReplicaDeletionStarted
          replicaState.put(partitionAndReplica, ReplicaDeletionStarted)
          // send stop replica command 发送停止副本命令
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = true,
            callbacks.stopReplicaResponseCallback)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case ReplicaDeletionIneligible =>
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionStarted), targetState)
          //设置状态为ReplicaDeletionIneligible
          replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case ReplicaDeletionSuccessful =>
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionStarted), targetState)
          //设置状态为ReplicaDeletionSuccessful
          replicaState.put(partitionAndReplica, ReplicaDeletionSuccessful)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case NonExistentReplica =>
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionSuccessful), targetState)
          // remove this replica from the assigned replicas list for its partition 从此分区的AR列表中删除此副本
          //获取此主题分区的当前AR列表
          val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
          //重新设置此主题分区的AR列表，排除当前副本ID
          controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas.filterNot(_ == replicaId))
          //副本状态存储删除此分区副本
          replicaState.remove(partitionAndReplica)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case OnlineReplica =>
          assertValidPreviousStates(partitionAndReplica,
            List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState)
          //获取当前状态
          replicaState(partitionAndReplica) match {
            //旧状态为NewReplica
            case NewReplica =>
              // add this replica to the assigned replicas list for its partition 添加此副本至此分区的AR列表
              val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
              if(!currentAssignedReplicas.contains(replicaId))
                controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas :+ replicaId)//此副本ID加入当前分区AR列表
              stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                                        .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState,
                                                targetState))
            case _ =>
              // check if the leader for this partition ever existed 检查此分区的Leader是否存在
              controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
                case Some(leaderIsrAndControllerEpoch) =>
                  brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId), topic, partition, leaderIsrAndControllerEpoch,
                    replicaAssignment)
                  replicaState.put(partitionAndReplica, OnlineReplica)
                  stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                    .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                case None => // that means the partition was never in OnlinePartition state, this means the broker never
                  // started a log for that partition and does not have a high watermark value for this partition
              }
          }
          replicaState.put(partitionAndReplica, OnlineReplica)
        case OfflineReplica =>
          assertValidPreviousStates(partitionAndReplica,
            List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState)
          // send stop replica command to the replica so that it stops fetching from the leader 向复制副本发送stop replica命令，使其停止从Leader获取
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = false)
          // As an optimization, the controller removes dead replicas from the ISR 作为一种优化，控制器从ISR中删除失效副本
          val leaderAndIsrIsEmpty: Boolean =
            controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
              case Some(currLeaderIsrAndControllerEpoch) =>
                //控制器从ISR删除副本
                controller.removeReplicaFromIsr(topic, partition, replicaId) match {
                  //删除成功
                  case Some(updatedLeaderIsrAndControllerEpoch) =>
                    // send the shrunk ISR state change request to all the remaining alive replicas of the partition. 将收缩的ISR状态更改请求发送到分区的所有剩余活动副本。
                    val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
                    if (!controller.deleteTopicManager.isPartitionToBeDeleted(topicAndPartition)) {
                      //剔除replicaId
                      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(currentAssignedReplicas.filterNot(_ == replicaId),
                        topic, partition, updatedLeaderIsrAndControllerEpoch, replicaAssignment)
                    }
                    //设置状态为OfflineReplica
                    replicaState.put(partitionAndReplica, OfflineReplica)
                    stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                      .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                    false
                  case None =>
                    true
                }
              case None =>
                true
            }
          if (leaderAndIsrIsEmpty && !controller.deleteTopicManager.isPartitionToBeDeleted(topicAndPartition))
            throw new StateChangeFailedException(
              "Failed to change state of replica %d for partition %s since the leader and isr path in zookeeper is empty"
              .format(replicaId, topicAndPartition))
      }
    }
    catch {
      case t: Throwable =>
        stateChangeLogger.error("Controller %d epoch %d initiated state change of replica %d for partition [%s,%d] from %s to %s failed"
                                  .format(controllerId, controller.epoch, replicaId, topic, partition, currState, targetState), t)
    }
  }

  /**
   * 此主题是否所有副本被删除
   * @param topic
   * @return
   */
  def areAllReplicasForTopicDeleted(topic: String): Boolean = {
    val replicasForTopic = controller.controllerContext.replicasForTopic(topic)
    val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap
    debug("Are all replicas for topic %s deleted %s".format(topic, replicaStatesForTopic))
    replicaStatesForTopic.forall(_._2 == ReplicaDeletionSuccessful)
  }

  /**
   * 是否至少有一个复制副本处于删除已启动状态
   * @param topic
   * @return
   */
  def isAtLeastOneReplicaInDeletionStartedState(topic: String): Boolean = {
    val replicasForTopic = controller.controllerContext.replicasForTopic(topic)
    val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap
    replicaStatesForTopic.foldLeft(false)((deletionState, r) => deletionState || r._2 == ReplicaDeletionStarted)
  }

  def replicasInState(topic: String, state: ReplicaState): Set[PartitionAndReplica] = {
    replicaState.filter(r => r._1.topic.equals(topic) && r._2 == state).keySet
  }

  def isAnyReplicaInState(topic: String, state: ReplicaState): Boolean = {
    replicaState.exists(r => r._1.topic.equals(topic) && r._2 == state)
  }

  def replicasInDeletionStates(topic: String): Set[PartitionAndReplica] = {
    val deletionStates = Set(ReplicaDeletionStarted, ReplicaDeletionSuccessful, ReplicaDeletionIneligible)
    replicaState.filter(r => r._1.topic.equals(topic) && deletionStates.contains(r._2)).keySet
  }

  private def assertValidPreviousStates(partitionAndReplica: PartitionAndReplica, fromStates: Seq[ReplicaState],
                                        targetState: ReplicaState) {
    assert(fromStates.contains(replicaState(partitionAndReplica)),
      "Replica %s should be in the %s states before moving to %s state"
        .format(partitionAndReplica, fromStates.mkString(","), targetState) +
        ". Instead it is in %s state".format(replicaState(partitionAndReplica)))
  }

  private def registerBrokerChangeListener() = {
    zkUtils.zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
  }

  private def deregisterBrokerChangeListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
  }

  /**
   * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
   * in zookeeper
   * 在副本的状态机启动时调用，以设置zk中所有现有分区的副本的初始状态
   */
  private def initializeReplicaState() {
    //遍历"主题分区:AR列表"的map
    for((topicPartition, assignedReplicas) <- controllerContext.partitionReplicaAssignment) {
      val topic = topicPartition.topic
      val partition = topicPartition.partition
      //遍历AR列表
      assignedReplicas.foreach { replicaId =>
        val partitionAndReplica = PartitionAndReplica(topic, partition, replicaId)
        //在线BrokerIds是否存在当前副本id
        controllerContext.liveBrokerIds.contains(replicaId) match {
          //如果存在，设置状态为OnlineReplica
          case true => replicaState.put(partitionAndReplica, OnlineReplica)
          case false =>
            // mark replicas on dead brokers as failed for topic deletion, if they belong to a topic to be deleted.
            // This is required during controller failover since during controller failover a broker can go down,
            // so the replicas on that broker should be moved to ReplicaDeletionIneligible to be on the safer side.
            replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)//设置状态为ReplicaDeletionIneligible
        }
      }
    }
  }

  def partitionsAssignedToBroker(topics: Seq[String], brokerId: Int):Seq[TopicAndPartition] = {
    controllerContext.partitionReplicaAssignment.filter(_._2.contains(brokerId)).keySet.toSeq
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a replica 这是zk监听器，用于触发副本的所有状态转换
   * broker状态变化监听器，有新增或移除的broker调用
   */
  class BrokerChangeListener() extends IZkChildListener with Logging {
    this.logIdent = "[BrokerChangeListener on Controller " + controller.config.brokerId + "]: "
    def handleChildChange(parentPath : String, currentBrokerList : java.util.List[String]) {
      info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.sorted.mkString(",")))
      inLock(controllerContext.controllerLock) {
        if (hasStarted.get) {
          ControllerStats.leaderElectionTimer.time {
            try {
              val curBrokers = currentBrokerList.map(_.toInt).toSet.flatMap(zkUtils.getBrokerInfo)
              val curBrokerIds = curBrokers.map(_.id)
              val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
              val newBrokerIds = curBrokerIds -- liveOrShuttingDownBrokerIds
              val deadBrokerIds = liveOrShuttingDownBrokerIds -- curBrokerIds
              val newBrokers = curBrokers.filter(broker => newBrokerIds(broker.id))
              controllerContext.liveBrokers = curBrokers
              val newBrokerIdsSorted = newBrokerIds.toSeq.sorted
              val deadBrokerIdsSorted = deadBrokerIds.toSeq.sorted
              val liveBrokerIdsSorted = curBrokerIds.toSeq.sorted
              info("Newly added brokers: %s, deleted brokers: %s, all live brokers: %s"
                .format(newBrokerIdsSorted.mkString(","), deadBrokerIdsSorted.mkString(","), liveBrokerIdsSorted.mkString(",")))
              newBrokers.foreach(controllerContext.controllerChannelManager.addBroker)
              deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker)
              if(newBrokerIds.size > 0)
                controller.onBrokerStartup(newBrokerIdsSorted)
              if(deadBrokerIds.size > 0)
                controller.onBrokerFailure(deadBrokerIdsSorted)
            } catch {
              case e: Throwable => error("Error while handling broker changes", e)
            }
          }
        }
      }
    }
  }
}

/**
 * 分区副本状态
 */
sealed trait ReplicaState { def state: Byte }
//副本刚被分配，但是还没有开始工作时候的状态；
case object NewReplica extends ReplicaState { val state: Byte = 1 }
//代表分区剧本开始作时的状态，此时该副本是该分区的 Leader 或者 Follower
case object OnlineReplica extends ReplicaState { val state: Byte = 2 }
//代表分区副本所在的Broker Server宕机时所导致的副本状态
case object OfflineReplica extends ReplicaState { val state: Byte = 3 }
//代表分区副本下线之准备 开始删除的状态
case object ReplicaDeletionStarted extends ReplicaState { val state: Byte = 4}
//代表相关Broker Server正确响应分区副本被删除请求之后的状态
case object ReplicaDeletionSuccessful extends ReplicaState { val state: Byte = 5}
//代表相关Broker Server错误响应分区副本被删除请求之后的状态
case object ReplicaDeletionIneligible extends ReplicaState { val state: Byte = 6}
//代表分区副本被彻底删除之后的状态
case object NonExistentReplica extends ReplicaState { val state: Byte = 7 }
