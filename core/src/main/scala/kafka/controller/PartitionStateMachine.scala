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
import collection.JavaConversions
import collection.mutable.Buffer
import java.util.concurrent.atomic.AtomicBoolean
import kafka.api.LeaderAndIsr
import kafka.common.{LeaderElectionNotNeededException, TopicAndPartition, StateChangeFailedException, NoReplicaOnlineException}
import kafka.utils.{Logging, ReplicationUtils}
import kafka.utils.ZkUtils._
import org.I0Itec.zkclient.{IZkDataListener, IZkChildListener}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import kafka.controller.Callbacks.CallbackBuilder
import kafka.utils.CoreUtils._

/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and 此类表示分区的状态机。它定义了分区可以处于的状态，以及将分区移动到另一个合法状态的转换。
 * transitions to move the partition to another legal state. The different states that a partition can be in are - 分区可以处于的不同状态是-
 * 1. NonExistentPartition: This state indicates that the partition was either never created or was created and then
 *                          deleted. Valid previous state, if one exists, is OfflinePartition 此状态表示该分区从未创建过，或者创建后被删除。以前的有效状态（如果存在）为OfflinePartition
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have 创建后，分区处于NewPartition状态。
 *                          replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition 在这种状态下，分区应该有分配给它的副本，但还没有leader和isr。以前的有效状态为NonExistentPartition
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 *                          Valid previous states are NewPartition/OfflinePartition 一旦为分区选择了一个领导者，它就处于OnlinePartition状态。以前的有效状态为NewPartitionOfflinePartition
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition 如果在领导人选举成功后，分区的领导人死亡，则分区将移动到OfflinePartition状态。
 *                          moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition 以前的有效状态为NewPartition和OnlinePartition
 */
class PartitionStateMachine(controller: KafkaController) extends Logging {
  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId
  private val zkUtils = controllerContext.zkUtils
  private val partitionState: mutable.Map[TopicAndPartition, PartitionState] = mutable.Map.empty
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(controller)
  private val hasStarted = new AtomicBoolean(false)
  //默认使用NoOpLeaderSelector
  private val noOpPartitionLeaderSelector = new NoOpLeaderSelector(controllerContext)
  //
  private val topicChangeListener = new TopicChangeListener()
  private val deleteTopicsListener = new DeleteTopicsListener()
  private val partitionModificationsListeners: mutable.Map[String, PartitionModificationsListener] = mutable.Map.empty
  private val stateChangeLogger = KafkaController.stateChangeLogger

  this.logIdent = "[Partition state machine on Controller " + controllerId + "]: "

  /**
   * Invoked on successful controller election. First registers a topic change listener since that triggers all
   * state transitions for partitions. Initializes the state of partitions by reading from zookeeper. Then triggers
   * the OnlinePartition state change for all new or offline partitions.
   */
  def startup() {
    // initialize partition state 初始化分区状态
    initializePartitionState()
    // set started flag 设置已启动标记
    hasStarted.set(true)
    // try to move partitions to online state 设置分区状态为在线状态
    triggerOnlinePartitionStateChange()

    info("Started partition state machine with initial state -> " + partitionState.toString())
  }

  // register topic and partition change listeners 注册主题分区变化监听器
  def registerListeners() {
    registerTopicChangeListener()
    if(controller.config.deleteTopicEnable)
      registerDeleteTopicListener()
  }

  // de-register topic and partition change listeners 取消注册主题分区变化监听器
  def deregisterListeners() {
    deregisterTopicChangeListener()
    partitionModificationsListeners.foreach {
      case (topic, listener) =>
        zkUtils.zkClient.unsubscribeDataChanges(getTopicPath(topic), listener)
    }
    partitionModificationsListeners.clear()
    if(controller.config.deleteTopicEnable)
      deregisterDeleteTopicListener()
  }

  /**
   * Invoked on controller shutdown. 控制器关闭时调用
   */
  def shutdown() {
    // reset started flag 重置已启动标志
    hasStarted.set(false)
    // clear partition state 清除分区状态
    partitionState.clear()
    // de-register all ZK listeners 取消注册所有zk监听器
    deregisterListeners()

    info("Stopped partition state machine")
  }

  /**
   * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
   * state. This is called on a successful controller election and on broker changes
   * 此API在NewPartition或OfflinePartition状态下的所有分区上调用OnlinePartition状态更改。这在成功的控制器选举和代理更改时调用
   */
  def triggerOnlinePartitionStateChange() {
    try {
      brokerRequestBatch.newBatch()
      // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state except partitions
      // that belong to topics to be deleted
      for((topicAndPartition, partitionState) <- partitionState
          if(!controller.deleteTopicManager.isTopicQueuedUpForDeletion(topicAndPartition.topic))) {
        if(partitionState.equals(OfflinePartition) || partitionState.equals(NewPartition))
          handleStateChange(topicAndPartition.topic, topicAndPartition.partition, OnlinePartition, controller.offlinePartitionSelector,
                            (new CallbackBuilder).build)
      }
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
    } catch {
      case e: Throwable => error("Error while moving some partitions to the online state", e)
      // TODO: It is not enough to bail out and log an error, it is important to trigger leader election for those partitions
    }
  }

  def partitionsInState(state: PartitionState): Set[TopicAndPartition] = {
    partitionState.filter(p => p._2 == state).keySet
  }

  /**
   * This API is invoked by the partition change zookeeper listener 此API由分区变化zk监听器调用
   * @param partitions   The list of partitions that need to be transitioned to the target state 分区列表需要更新成目标状态
   * @param targetState  The state that the partitions should be moved to 目标状态
   */
  def handleStateChanges(partitions: Set[TopicAndPartition], targetState: PartitionState,
                         leaderSelector: PartitionLeaderSelector = noOpPartitionLeaderSelector,
                         callbacks: Callbacks = (new CallbackBuilder).build) {
    info("Invoking state change to %s for partitions %s".format(targetState, partitions.mkString(",")))
    try {
      brokerRequestBatch.newBatch()
      partitions.foreach { topicAndPartition =>
        handleStateChange(topicAndPartition.topic, topicAndPartition.partition, targetState, leaderSelector, callbacks)
      }
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
    }catch {
      case e: Throwable => error("Error while moving some partitions to %s state".format(targetState), e)
      // TODO: It is not enough to bail out and log an error, it is important to trigger state changes for those partitions
    }
  }

  /**
   * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentPartition -> NewPartition:
   * --load assigned replicas from ZK to controller cache 将分AR从ZK加载到控制器缓存
   *
   * NewPartition -> OnlinePartition
   * --assign first live replica as the leader and all live replicas as the isr; write leader and isr to ZK for this partition 将第一个活动副本分配为Leader，将所有在线副本分配为isr；为此分区将leader和isr写入ZK
   * --send LeaderAndIsr request to every live replica and UpdateMetadata request to every live broker 向每个活动复制副本发送LeaderAndIsr请求，并向每个live broker发送UpdateMetadata请求
   *
   * OnlinePartition,OfflinePartition -> OnlinePartition
   * --select new leader and isr for this partition and a set of replicas to receive the LeaderAndIsr request, and write leader and isr to ZK 为该分区和一组副本选择新的leader和isr以接收LeaderAndIsr请求，并将leader和isr写入ZK
   * --for this partition, send LeaderAndIsr request to every receiving replica and UpdateMetadata request to every live broker 对于该分区，向每个接收副本发送LeaderAndIsr请求，并向每个live broker发送UpdateMetadata请求
   *
   * NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition
   * --nothing other than marking partition state as Offline 只有将分区状态标记为脱机
   *
   * OfflinePartition -> NonExistentPartition
   * --nothing other than marking the partition state as NonExistentPartition 只有将分区状态标记为不存在的分区
   * @param topic       The topic of the partition for which the state transition is invoked
   * @param partition   The partition for which the state transition is invoked
   * @param targetState The end state that the partition should be moved to
   */
  private def handleStateChange(topic: String, partition: Int, targetState: PartitionState,
                                leaderSelector: PartitionLeaderSelector,
                                callbacks: Callbacks) {
    val topicAndPartition = TopicAndPartition(topic, partition)
    if (!hasStarted.get)
      throw new StateChangeFailedException(("Controller %d epoch %d initiated state change for partition %s to %s failed because " +
                                            "the partition state machine has not started")
                                              .format(controllerId, controller.epoch, topicAndPartition, targetState))
    //获取主题分区当前状态，如果不存在则设置为NonExistentPartition
    val currState = partitionState.getOrElseUpdate(topicAndPartition, NonExistentPartition)
    try {
      targetState match {
        //目标状态是NewPartition
        case NewPartition =>
          // pre: partition did not exist before this 在此之前不存在分区
          assertValidPreviousStates(topicAndPartition, List(NonExistentPartition), NewPartition)
          //主题分区状态设置为NewPartition
          partitionState.put(topicAndPartition, NewPartition)
          //此主题分区的AR列表
          val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).mkString(",")
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s with assigned replicas %s"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState,
                                            assignedReplicas))
          // post: partition has been assigned replicas 分区已经分配过副本
        //目标状态是OnlinePartition
        case OnlinePartition =>
          assertValidPreviousStates(topicAndPartition, List(NewPartition, OnlinePartition, OfflinePartition), OnlinePartition)
          //获取当前状态-旧状态
          partitionState(topicAndPartition) match {
            //旧状态是NewPartition
            case NewPartition =>
              // initialize leader and isr path for new partition 为这个新分区初始化Leader和ISR列表
              initializeLeaderAndIsrForPartition(topicAndPartition)
            //旧状态是OfflinePartition
            case OfflinePartition =>
              //选举新Leader、ISR和控制器纪元
              electLeaderForPartition(topic, partition, leaderSelector)
            case OnlinePartition => // invoked when the leader needs to be re-elected 当Leader需要重新选举时
              electLeaderForPartition(topic, partition, leaderSelector)
            case _ => // should never come here since illegal previous states are checked above
          }
          //设置状态为OnlinePartition
          partitionState.put(topicAndPartition, OnlinePartition)
          val leader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s from %s to %s with leader %d"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState, leader))
           // post: partition has a leader
        //分区已经有一个Leader
        case OfflinePartition =>
          // pre: partition should be in New or Online state 分区应该是新建或在线状态
          assertValidPreviousStates(topicAndPartition, List(NewPartition, OnlinePartition, OfflinePartition), OfflinePartition)
          // should be called when the leader for a partition is no longer alive 当分区的前导不再活动时应调用
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState))
          partitionState.put(topicAndPartition, OfflinePartition)
          // post: partition has no alive leader 分区没有活动的Leader
        case NonExistentPartition =>
          // pre: partition should be in Offline state 分区应处于脱机状态
          assertValidPreviousStates(topicAndPartition, List(OfflinePartition), NonExistentPartition)
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState))
          partitionState.put(topicAndPartition, NonExistentPartition)
          // post: partition state is deleted from all brokers and zookeeper 从所有代理和zookeeper中删除分区状态
      }
    } catch {
      case t: Throwable =>
        stateChangeLogger.error("Controller %d epoch %d initiated state change for partition %s from %s to %s failed"
          .format(controllerId, controller.epoch, topicAndPartition, currState, targetState), t)
    }
  }

  /**
   * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
   * zookeeper在启动分区的状态机时调用，以设置zookeeper中所有现有分区的初始状态
   */
  private def initializePartitionState() {
    //遍历 主题分区:AR列表 map
    for((topicPartition, replicaAssignment) <- controllerContext.partitionReplicaAssignment) {
      // check if leader and isr path exists for partition. If not, then it is in NEW state 检查每个分区Leader和ISR路径是否存在数据，如果不存在则是NEW状态
      controllerContext.partitionLeadershipInfo.get(topicPartition) match {
        //如果存在
        case Some(currentLeaderIsrAndEpoch) =>
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
          //检查副本Leader是否存活，如果是，则是在线状态，否则是离线状态
          controllerContext.liveBrokerIds.contains(currentLeaderIsrAndEpoch.leaderAndIsr.leader) match {
            //Leader在线
            case true => // leader is alive
              partitionState.put(topicPartition, OnlinePartition)
            //Leader离线
            case false =>
              partitionState.put(topicPartition, OfflinePartition)
          }
        //不存在，设置为NewPartition
        case None =>
          partitionState.put(topicPartition, NewPartition)
      }
    }
  }

  private def assertValidPreviousStates(topicAndPartition: TopicAndPartition, fromStates: Seq[PartitionState],
                                        targetState: PartitionState) {
    if(!fromStates.contains(partitionState(topicAndPartition)))
      throw new IllegalStateException("Partition %s should be in the %s states before moving to %s state"
        .format(topicAndPartition, fromStates.mkString(","), targetState) + ". Instead it is in %s state"
        .format(partitionState(topicAndPartition)))
  }

  /**
   * Invoked on the NewPartition->OnlinePartition state change. When a partition is in the New state, it does not have
   * a leader and isr path in zookeeper. Once the partition moves to the OnlinePartition state, its leader and isr
   * path gets initialized and it never goes back to the NewPartition state. From here, it can only go to the
   * OfflinePartition state.
   * 在NewPartition->OnlinePartition状态更改时调用。分区处于新状态时，zookeeper中没有引线和isr路径。
   * 一旦分区移动到OnlinePartition状态，它的Leader和ISR路径将被初始化，并且永远不会返回到NewPartition状态。
   * 从这里开始，它只能进入OfflinePartition状态。
   * @param topicAndPartition   The topic/partition whose leader and isr path is to be initialized
   */
  private def initializeLeaderAndIsrForPartition(topicAndPartition: TopicAndPartition) {
    //从控制器上下文获取主题分区AR列表
    val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
    //过滤在线的AR列表
    val liveAssignedReplicas = replicaAssignment.filter(r => controllerContext.liveBrokerIds.contains(r))
    liveAssignedReplicas.size match {
      case 0 =>
        val failMsg = ("encountered error during state change of partition %s from New to Online, assigned replicas are [%s], " +
                       "live brokers are [%s]. No assigned replica is alive.")
                         .format(topicAndPartition, replicaAssignment.mkString(","), controllerContext.liveBrokerIds)
        stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
        throw new StateChangeFailedException(failMsg)
      case _ =>
        debug("Live assigned replicas for partition %s are: [%s]".format(topicAndPartition, liveAssignedReplicas))
        // make the first replica in the list of assigned replicas, the leader 创建已分配副本列表中的第一个副本，即领导者
        val leader = liveAssignedReplicas.head
        //设置新的Leader、ISR列表、Leader纪元和控制器纪元
        val leaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader, liveAssignedReplicas.toList),
          controller.epoch)
        debug("Initializing leader and isr for partition %s to %s".format(topicAndPartition, leaderIsrAndControllerEpoch))
        try {
          //写入zk 路径/topic/partionId/state
          zkUtils.createPersistentPath(
            getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
            zkUtils.leaderAndIsrZkData(leaderIsrAndControllerEpoch.leaderAndIsr, controller.epoch))
          // NOTE: the above write can fail only if the current controller lost its zk session and the new controller
          // took over and initialized this partition. This can happen if the current controller went into a long
          // GC pause
          //设置主题分区的Leader、ISR列表、Leader纪元和控制器纪元信息
          controllerContext.partitionLeadershipInfo.put(topicAndPartition, leaderIsrAndControllerEpoch)
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(liveAssignedReplicas, topicAndPartition.topic,
            topicAndPartition.partition, leaderIsrAndControllerEpoch, replicaAssignment)
        } catch {
          case e: ZkNodeExistsException =>
            // read the controller epoch
            val leaderIsrAndEpoch = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topicAndPartition.topic,
              topicAndPartition.partition).get
            val failMsg = ("encountered error while changing partition %s's state from New to Online since LeaderAndIsr path already " +
                           "exists with value %s and controller epoch %d")
                             .format(topicAndPartition, leaderIsrAndEpoch.leaderAndIsr.toString(), leaderIsrAndEpoch.controllerEpoch)
            stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
            throw new StateChangeFailedException(failMsg)
        }
    }
  }

  /**
   * Invoked on the OfflinePartition,OnlinePartition->OnlinePartition state change. 在OfflinePartition、OnlinePartition->OnlinePartition状态更改上调用。
   * It invokes the leader election API to elect a leader for the input offline partition 它调用Leader选举API为输入脱机分区选择一个Leader
   * @param topic               The topic of the offline partition
   * @param partition           The offline partition
   * @param leaderSelector      Specific leader selector (e.g., offline/reassigned/etc.)
   */
  def electLeaderForPartition(topic: String, partition: Int, leaderSelector: PartitionLeaderSelector) {
    val topicAndPartition = TopicAndPartition(topic, partition)
    // handle leader election for the partitions whose leader is no longer alive 处理其领导人不再活动的分区的领导人选举
    stateChangeLogger.trace("Controller %d epoch %d started leader election for partition %s"
                              .format(controllerId, controller.epoch, topicAndPartition))
    try {
      var zookeeperPathUpdateSucceeded: Boolean = false
      var newLeaderAndIsr: LeaderAndIsr = null
      var replicasForThisPartition: Seq[Int] = Seq.empty[Int]
      while(!zookeeperPathUpdateSucceeded) {
        //获取当前Leader和ISR以及控制器纪元
        val currentLeaderIsrAndEpoch = getLeaderIsrAndEpochOrThrowException(topic, partition)
        val currentLeaderAndIsr = currentLeaderIsrAndEpoch.leaderAndIsr
        val controllerEpoch = currentLeaderIsrAndEpoch.controllerEpoch
        if (controllerEpoch > controller.epoch) {
          val failMsg = ("aborted leader election for partition [%s,%d] since the LeaderAndIsr path was " +
                         "already written by another controller. This probably means that the current controller %d went through " +
                         "a soft failure and another controller was elected with epoch %d.")
                           .format(topic, partition, controllerId, controllerEpoch)
          stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
          throw new StateChangeFailedException(failMsg)
        }
        // elect new leader or throw exception 选择新领导或抛出异常
        val (leaderAndIsr, replicas) = leaderSelector.selectLeader(topicAndPartition, currentLeaderAndIsr)
        val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partition,
          leaderAndIsr, controller.epoch, currentLeaderAndIsr.zkVersion)
        newLeaderAndIsr = leaderAndIsr
        newLeaderAndIsr.zkVersion = newVersion
        zookeeperPathUpdateSucceeded = updateSucceeded
        replicasForThisPartition = replicas
      }
      val newLeaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(newLeaderAndIsr, controller.epoch)
      // update the leader cache 更新Leader缓存
      controllerContext.partitionLeadershipInfo.put(TopicAndPartition(topic, partition), newLeaderIsrAndControllerEpoch)
      stateChangeLogger.trace("Controller %d epoch %d elected leader %d for Offline partition %s"
                                .format(controllerId, controller.epoch, newLeaderAndIsr.leader, topicAndPartition))
      val replicas = controllerContext.partitionReplicaAssignment(TopicAndPartition(topic, partition))
      // store new leader and isr info in cache
      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasForThisPartition, topic, partition,
        newLeaderIsrAndControllerEpoch, replicas)
    } catch {
      case lenne: LeaderElectionNotNeededException => // swallow
      case nroe: NoReplicaOnlineException => throw nroe
      case sce: Throwable =>
        val failMsg = "encountered error while electing leader for partition %s due to: %s.".format(topicAndPartition, sce.getMessage)
        stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
        throw new StateChangeFailedException(failMsg, sce)
    }
    debug("After leader election, leader cache is updated to %s".format(controllerContext.partitionLeadershipInfo.map(l => (l._1, l._2))))
  }

  private def registerTopicChangeListener() = {
    zkUtils.zkClient.subscribeChildChanges(BrokerTopicsPath, topicChangeListener)
  }

  private def deregisterTopicChangeListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(BrokerTopicsPath, topicChangeListener)
  }

  def registerPartitionChangeListener(topic: String) = {
    partitionModificationsListeners.put(topic, new PartitionModificationsListener(topic))
    zkUtils.zkClient.subscribeDataChanges(getTopicPath(topic), partitionModificationsListeners(topic))
  }

  def deregisterPartitionChangeListener(topic: String) = {
    zkUtils.zkClient.unsubscribeDataChanges(getTopicPath(topic), partitionModificationsListeners(topic))
    partitionModificationsListeners.remove(topic)
  }

  private def registerDeleteTopicListener() = {
    zkUtils.zkClient.subscribeChildChanges(DeleteTopicsPath, deleteTopicsListener)
  }

  private def deregisterDeleteTopicListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(DeleteTopicsPath, deleteTopicsListener)
  }

  private def getLeaderIsrAndEpochOrThrowException(topic: String, partition: Int): LeaderIsrAndControllerEpoch = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition) match {
      case Some(currentLeaderIsrAndEpoch) => currentLeaderIsrAndEpoch
      case None =>
        val failMsg = "LeaderAndIsr information doesn't exist for partition %s in %s state"
                        .format(topicAndPartition, partitionState(topicAndPartition))
        throw new StateChangeFailedException(failMsg)
    }
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a partition
   * 这是zookeeper监听器，用于触发分区的所有状态转换
   */
  class TopicChangeListener extends IZkChildListener with Logging {
    this.logIdent = "[TopicChangeListener on Controller " + controller.config.brokerId + "]: "

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, children : java.util.List[String]) {
      inLock(controllerContext.controllerLock) {
        if (hasStarted.get) {
          try {
            val currentChildren = {
              import JavaConversions._
              debug("Topic change listener fired for path %s with children %s".format(parentPath, children.mkString(",")))
              (children: Buffer[String]).toSet
            }
            //zk路径下所有主题 - 控制器上下文所有主题 = 新的主题
            val newTopics = currentChildren -- controllerContext.allTopics
            //控制器上下文所有主题 - zk路径下所有主题 = 要删除主题
            val deletedTopics = controllerContext.allTopics -- currentChildren
            //设置控制器上下文所有主题
            controllerContext.allTopics = currentChildren
            //获取主题的AR列表
            val addedPartitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(newTopics.toSeq)
            controllerContext.partitionReplicaAssignment = controllerContext.partitionReplicaAssignment.filter(p =>
              !deletedTopics.contains(p._1.topic))
            controllerContext.partitionReplicaAssignment.++=(addedPartitionReplicaAssignment)
            info("New topics: [%s], deleted topics: [%s], new partition replica assignment [%s]".format(newTopics,
              deletedTopics, addedPartitionReplicaAssignment))
            if(newTopics.size > 0)
              controller.onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet.toSet)
          } catch {
            case e: Throwable => error("Error while handling new topic", e )
          }
        }
      }
    }
  }

  /**
   * Delete topics includes the following operations - 删除主题包括以下操作
   * 1. Add the topic to be deleted to the delete topics cache, only if the topic exists 1、仅当主题存在时，才将要删除的主题添加到删除主题缓存
   * 2. If there are topics to be deleted, it signals the delete topic thread 2、如果有要删除的主题，它会向删除主题线程发出信号
   */
  class DeleteTopicsListener() extends IZkChildListener with Logging {
    this.logIdent = "[DeleteTopicsListener on " + controller.config.brokerId + "]: "
    val zkUtils = controllerContext.zkUtils

    /**
     * Invoked when a topic is being deleted 在删除主题时调用
     * @throws Exception On any error.
     */
    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, children : java.util.List[String]) {
      inLock(controllerContext.controllerLock) {
        var topicsToBeDeleted = {
          import JavaConversions._
          (children: Buffer[String]).toSet
        }
        debug("Delete topics listener fired for topics %s to be deleted".format(topicsToBeDeleted.mkString(",")))
        //控制器上下文所有主题集合中不存在的就是不存在的主题
        val nonExistentTopics = topicsToBeDeleted.filter(t => !controllerContext.allTopics.contains(t))
        if(nonExistentTopics.size > 0) {
          warn("Ignoring request to delete non-existing topics " + nonExistentTopics.mkString(","))
          //遍历删除zk上的主题路径
          nonExistentTopics.foreach(topic => zkUtils.deletePathRecursive(getDeleteTopicPath(topic)))
        }
        //所有主题排除掉不存在的主题
        topicsToBeDeleted --= nonExistentTopics
        if(topicsToBeDeleted.size > 0) {
          info("Starting topic deletion for topics " + topicsToBeDeleted.mkString(","))
          // mark topic ineligible for deletion if other state changes are in progress 如果正在进行其他状态更改，则将主题标记为不符合删除条件
          topicsToBeDeleted.foreach { topic =>
            //正在进行首选副本选择的分区
            val preferredReplicaElectionInProgress =
              controllerContext.partitionsUndergoingPreferredReplicaElection.map(_.topic).contains(topic)
            //正在重新分配的分区
            val partitionReassignmentInProgress =
              controllerContext.partitionsBeingReassigned.keySet.map(_.topic).contains(topic)
            if(preferredReplicaElectionInProgress || partitionReassignmentInProgress)
              controller.deleteTopicManager.markTopicIneligibleForDeletion(Set(topic))//将主题标记为不符合删除条件
          }
          // add topic to deletion list 将主题添加到删除列表
          controller.deleteTopicManager.enqueueTopicsForDeletion(topicsToBeDeleted)
        }
      }
    }

    /**
     *
     * @throws Exception
   *             On any error.
     */
    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
    }
  }

  /**
   * 分区修改监听器
   * @param topic
   */
  class PartitionModificationsListener(topic: String) extends IZkDataListener with Logging {

    this.logIdent = "[AddPartitionsListener on " + controller.config.brokerId + "]: "

    @throws(classOf[Exception])
    def handleDataChange(dataPath : String, data: Object) {
      inLock(controllerContext.controllerLock) {
        try {
          info(s"Partition modification triggered $data for path $dataPath")
          //获取主题分区的AR列表
          val partitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(List(topic))
          //过滤掉主题分区AR列表已经存在的副本就是要被添加的副本集合
          val partitionsToBeAdded = partitionReplicaAssignment.filter(p =>
            !controllerContext.partitionReplicaAssignment.contains(p._1))
          //主题是否已排队等待删除
          if(controller.deleteTopicManager.isTopicQueuedUpForDeletion(topic))
            error("Skipping adding partitions %s for topic %s since it is currently being deleted"
                  .format(partitionsToBeAdded.map(_._1.partition).mkString(","), topic))
          else {
            if (partitionsToBeAdded.size > 0) {
              info("New partitions to be added %s".format(partitionsToBeAdded))
              //要被添加的副本集合添加到控制器上下文的主题分区AR集合
              controllerContext.partitionReplicaAssignment.++=(partitionsToBeAdded)
              //
              controller.onNewPartitionCreation(partitionsToBeAdded.keySet.toSet)
            }
          }
        } catch {
          case e: Throwable => error("Error while handling add partitions for data path " + dataPath, e )
        }
      }
    }

    @throws(classOf[Exception])
    def handleDataDeleted(parentPath : String) {
      // this is not implemented for partition change
    }
  }
}

/**
 * 分区状态
 */
sealed trait PartitionState { def state: Byte }
//代表分区刚创建，并且包含了AR，但是此时Leader或者ISR还没有创建，也就是此时分区还是非活动状态，无法接收数据
case object NewPartition extends PartitionState { val state: Byte = 0 }
//代表分区的Leader 已经被 选举出来，并且此时已经产生了对应的ISR ，也就是此时分区是活动状态，可以接收数据
case object OnlinePartition extends PartitionState { val state: Byte = 1 }
//代表了分区的Leader 由于某种原因下线时导致分区暂时不可用的状态
case object OfflinePartition extends PartitionState { val state: Byte = 2 }
//代表分区从来没有被创建或者被创建之后又被删除的状态
case object NonExistentPartition extends PartitionState { val state: Byte = 3 }
