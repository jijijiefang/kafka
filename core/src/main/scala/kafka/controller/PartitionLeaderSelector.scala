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

import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.log.LogConfig
import kafka.utils.Logging
import kafka.common.{LeaderElectionNotNeededException, TopicAndPartition, StateChangeFailedException, NoReplicaOnlineException}
import kafka.server.{ConfigType, KafkaConfig}

/**
 * 分区副本领导者选择器
 */
trait PartitionLeaderSelector {

  /**
   * @param topicAndPartition          The topic and partition whose leader needs to be elected 需要被选举的主题分区
   * @param currentLeaderAndIsr        The current leader and isr of input partition read from zookeeper 从zk中读取的当前领导和ISR列表
   * @throws NoReplicaOnlineException If no replica in the assigned replicas list is alive 如果分配的副本列表中没有处于活动状态的副本
   * @return The leader and isr request, with the newly selected leader and isr, and the set of replicas to receive
   * the LeaderAndIsrRequest.
   */
  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int])

}

/**
 * 用于Offline状态Partitions的选主,比如Topic刚刚创建后
 * Select the new leader, new isr and receiving replicas (for the LeaderAndIsrRequest): 选择一个新的Leader，新的ISR和接收副本（对于Leader和ISR请求）
 * 1. If at least one broker from the isr is alive, it picks a broker from the live isr as the new leader and the live
 *    isr as the new isr. 如果ISR列表中至少有一个Broker存活，则从中选择一个作为新的Leader和新的ISR列表
 * 2. Else, if unclean leader election for the topic is disabled, it throws a NoReplicaOnlineException. 否则，如果该主题的不干净领导人选举被禁用，它将抛出NoReplicaOnlineException。
 * 3. Else, it picks some alive broker from the assigned replica list as the new leader and the new isr. 否则，它会从分配的副本列表中选择一些活动代理作为新的领导者和新的isr
 * 4. If no broker in the assigned replica list is alive, it throws a NoReplicaOnlineException 如果分配的副本列表中没有代理处于活动状态，则会抛出NoReplicaOnlineException
 * Replicas to receive LeaderAndIsr request = live assigned replicas 要接收LeaderAndIsr请求的副本=实时分配的副本
 * Once the leader is successfully registered in zookeeper, it updates the allLeaders cache 一旦领导者在zookeeper中成功注册，它将更新allLeaders缓存
 */
class OfflinePartitionLeaderSelector(controllerContext: ControllerContext, config: KafkaConfig)
  extends PartitionLeaderSelector with Logging {
  this.logIdent = "[OfflinePartitionLeaderSelector]: "

  /**
   * 选择Leader
   * @param topicAndPartition          The topic and partition whose leader needs to be elected 需要选举的主题和分区
   * @param currentLeaderAndIsr        The current leader and isr of input partition read from zookeeper 从zk读取的输入分区的当前Leader和ISR
   * @return The leader and isr request, with the newly selected leader and isr, and the set of replicas to receive
   * the LeaderAndIsrRequest.
   * 当KafkaController尝试将分区状态从OfflinePartition或NewPartition切换为OnlinePartition时会使用这种策略
   * 1、筛选出在线的ISR和在线的AR
   * 2、优先在线的ISR中选择，在想ISR列表不为空，则选择在线的ISR列表中第一个，结束选举
   * 3、在线ISR为空，根据unclean.leader.election.enable配置是否在线AR中选择，unclean.leader.election.enable代表是否允许不在ISR列表中选举Leader,默认为true。
   * 如果设置为true,选择在线AR列表中第一个，结束选举；如果AR列表为空选举失败。
   */
  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    controllerContext.partitionReplicaAssignment.get(topicAndPartition) match {
      case Some(assignedReplicas) =>
        //在线的AR
        val liveAssignedReplicas = assignedReplicas.filter(r => controllerContext.liveBrokerIds.contains(r))
        //在线的ISR
        val liveBrokersInIsr = currentLeaderAndIsr.isr.filter(r => controllerContext.liveBrokerIds.contains(r))
        //当前领导者纪元
        val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
        //当前zk版本
        val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion
        //ISR列表是否为空
        val newLeaderAndIsr = liveBrokersInIsr.isEmpty match {
          //为空则
          case true =>
            // Prior to electing an unclean (i.e. non-ISR) leader, ensure that doing so is not disallowed by the configuration
            // for unclean leader election.
            if (!LogConfig.fromProps(config.originals, AdminUtils.fetchEntityConfig(controllerContext.zkUtils,
              ConfigType.Topic, topicAndPartition.topic)).uncleanLeaderElectionEnable) {
              throw new NoReplicaOnlineException(("No broker in ISR for partition " +
                "%s is alive. Live brokers are: [%s],".format(topicAndPartition, controllerContext.liveBrokerIds)) +
                " ISR brokers are: [%s]".format(currentLeaderAndIsr.isr.mkString(",")))
            }

            debug("No broker in ISR is alive for %s. Pick the leader from the alive assigned replicas: %s"
              .format(topicAndPartition, liveAssignedReplicas.mkString(",")))
            //遍历AR,是否为空
            liveAssignedReplicas.isEmpty match {
              //为空，抛出NoReplicaOnlineException异常
              case true =>
                throw new NoReplicaOnlineException(("No replica for partition " +
                  "%s is alive. Live brokers are: [%s],".format(topicAndPartition, controllerContext.liveBrokerIds)) +
                  " Assigned replicas are: [%s]".format(assignedReplicas))
              //不为空，AR列表第一个作为新的领导者
              case false =>
                ControllerStats.uncleanLeaderElectionRate.mark()
                val newLeader = liveAssignedReplicas.head
                warn("No broker in ISR is alive for %s. Elect leader %d from live brokers %s. There's potential data loss."
                     .format(topicAndPartition, newLeader, liveAssignedReplicas.mkString(",")))
                new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, List(newLeader), currentLeaderIsrZkPathVersion + 1)
            }
          //ISR列表不为空
          case false =>
            //在AR中过滤ISR
            val liveReplicasInIsr = liveAssignedReplicas.filter(r => liveBrokersInIsr.contains(r))
            //ISR列表第一个作为新的领导者
            val newLeader = liveReplicasInIsr.head
            debug("Some broker in ISR is alive for %s. Select %d from ISR %s to be the leader."
                  .format(topicAndPartition, newLeader, liveBrokersInIsr.mkString(",")))
            new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, liveBrokersInIsr.toList, currentLeaderIsrZkPathVersion + 1)
        }
        info("Selected new leader and ISR %s for offline partition %s".format(newLeaderAndIsr.toString(), topicAndPartition))
        (newLeaderAndIsr, liveAssignedReplicas)
      case None =>
        throw new NoReplicaOnlineException("Partition %s doesn't have replicas assigned to it".format(topicAndPartition))
    }
  }
}

/**
 * New leader = a live in-sync reassigned replica 一个活动的在同步状态下的分配的副本
 * New isr = current isr 当前ISR列表
 * Replicas to receive LeaderAndIsr request = reassigned replicas 要接收LeaderAndIsr请求的副本 = 重新分配的副本
 * 分区重分配时，在zk的/admin/reassign_partitions目录下指定主题分区的AR列表，此时KafkaController检测到数据变化，就会触发回调函数，促使对应主题分区进行选举
 * 1、获取指定AR列表；
 * 2、针对指定AR列表，在线Broker Server和当前ISR列表取交集；
 * 3、如果交集不为空，则选举成功，第一个副本即为新Leader；否则选举失败。
 */
class ReassignedPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {
  this.logIdent = "[ReassignedPartitionLeaderSelector]: "

  /**
   * The reassigned replicas are already in the ISR when selectLeader is called. selectLeader方法被调用时，重新分配的副本已在ISR列表中
   */
  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    //重新分配的ISR副本列表
    val reassignedInSyncReplicas = controllerContext.partitionsBeingReassigned(topicAndPartition).newReplicas
    //当前Leader纪元
    val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
    //当前zk版本
    val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion
    //在线的Broker Server和当前ISR列表取交集再与重新分配的ISR列表取交集
    val aliveReassignedInSyncReplicas = reassignedInSyncReplicas.filter(r => controllerContext.liveBrokerIds.contains(r) &&
                                                                             currentLeaderAndIsr.isr.contains(r))
    //列表第一个
    val newLeaderOpt = aliveReassignedInSyncReplicas.headOption
    newLeaderOpt match {
      //存在，选举为新Leader返回
      case Some(newLeader) => (new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, currentLeaderAndIsr.isr,
        currentLeaderIsrZkPathVersion + 1), reassignedInSyncReplicas)
      //不存在
      case None =>
        reassignedInSyncReplicas.size match {
          //重新分配的ISR副本列表元素为空
          case 0 =>
            throw new NoReplicaOnlineException("List of reassigned replicas for partition " +
              " %s is empty. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
          //没有匹配上
          case _ =>
            throw new NoReplicaOnlineException("None of the reassigned replicas for partition " +
              "%s are in-sync with the leader. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
        }
    }
  }
}

/**
 * New leader = preferred (first assigned) replica (if in isr and alive); 新领导者 = 首选（首次分配）副本（如果在isr中且处于活动状态）；
 * New isr = current isr; 新的ISR = 当前ISR
 * Replicas to receive LeaderAndIsr request = assigned replicas 接收LeaderAndIsr请求的副本 = AR
 * 如果配置auto.leader.rebalance.enable=true会自动触发分区Leader副本选举；或者管理员下发Leader副本选举指令。
 * 在zk的/admin/preferred_replica_election指定具体的主题分区，KafkaController监听到这个路径下数据发生变化，会触发回调函数，促使对应的主题分区发生Leader副本选举。
 * 1、获取分区原始AR列表；
 * 2、获取第一个副本作为待定Leader副本；
 * 3、判断待定Leader副本是否在在线的BrokerServer中和当前ISR列表中，如果是则选举成功，即第一个副本作为Leader；否则选举失败；
 */
class PreferredReplicaPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector
with Logging {
  this.logIdent = "[PreferredReplicaPartitionLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    //获取当前的AR列表
    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    //AR列表第一个副本作为待定的Leader副本
    val preferredReplica = assignedReplicas.head
    // check if preferred replica is the current leader 判断当前待定副本是否已经是当前Leader
    val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
    if (currentLeader == preferredReplica) {
      throw new LeaderElectionNotNeededException("Preferred replica %d is already the current leader for partition %s"
                                                   .format(preferredReplica, topicAndPartition))
    } else {
      info("Current leader %d for partition %s is not the preferred replica.".format(currentLeader, topicAndPartition) +
        " Trigerring preferred replica leader election")
      // check if preferred replica is not the current leader and is alive and in the isr 判断当前待定副本不是当前Leader且位于在线的BrokerServer中且在ISR列表中
      if (controllerContext.liveBrokerIds.contains(preferredReplica) && currentLeaderAndIsr.isr.contains(preferredReplica)) {
        //成为Leader返回
        (new LeaderAndIsr(preferredReplica, currentLeaderAndIsr.leaderEpoch + 1, currentLeaderAndIsr.isr,
          currentLeaderAndIsr.zkVersion + 1), assignedReplicas)
      } else {
        throw new StateChangeFailedException("Preferred replica %d for partition ".format(preferredReplica) +
          "%s is either not alive or not in the isr. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
      }
    }
  }
}

/**
 * 用于ControllerShutdown时的leader select
 * New leader = replica in isr that's not being shutdown; 新的Leader = ISR中未关闭的副本
 * New isr = current isr - shutdown replica; 新的ISR = 当前ISR排除关闭的副本
 * Replicas to receive LeaderAndIsr request = live assigned replicas 要接收LeaderAndIsr请求的副本 = 在线的AR
 * 当BrokerServer下线时向KafkaController发送ControlledShutdownRequest指令，KafkaController收到指令后对位于该BrokerServer上的Leader副本进行重新Leader副本选举。
 * 1、获取分区ISR列表；
 * 2、在ISR列表中剔除离线的ISR列表作为新的ISR列表；
 * 3、如果新的ISR列表不为空，则选举成功，第一个副本即为新的Leader；否则选举失败；
 */
class ControlledShutdownLeaderSelector(controllerContext: ControllerContext)
        extends PartitionLeaderSelector
        with Logging {

  this.logIdent = "[ControlledShutdownLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    //当前领导纪元
    val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
    //当前zk版本
    val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion
    //当前副本Leader
    val currentLeader = currentLeaderAndIsr.leader
    //获取AR列表
    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    //在线的BrokerServer列表
    val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
    //过滤Broker后的AR列表
    val liveAssignedReplicas = assignedReplicas.filter(r => liveOrShuttingDownBrokerIds.contains(r))
    //新的ISR列表 = 当前ISR列表去除下线Broker Server的
    val newIsr = currentLeaderAndIsr.isr.filter(brokerId => !controllerContext.shuttingDownBrokerIds.contains(brokerId))
    //AR中是否存在ISR中第一个副本
    liveAssignedReplicas.filter(newIsr.contains).headOption match {
      //存在，则作为新的Leader
      case Some(newLeader) =>
        debug("Partition %s : current leader = %d, new leader = %d".format(topicAndPartition, currentLeader, newLeader))
        (LeaderAndIsr(newLeader, currentLeaderEpoch + 1, newIsr, currentLeaderIsrZkPathVersion + 1), liveAssignedReplicas)
      case None =>
        throw new StateChangeFailedException(("No other replicas in ISR %s for %s besides" +
          " shutting down brokers %s").format(currentLeaderAndIsr.isr.mkString(","), topicAndPartition, controllerContext.shuttingDownBrokerIds.mkString(",")))
    }
  }
}

/**
 * Essentially does nothing. Returns the current leader and ISR, and the current
 * set of replicas assigned to a given topic/partition. 基本上什么都不做。返回当前的领导者和ISR，以及分配给给定主题/分区的当前副本集
 * 该策略为KafkaController内部默认的分区副本选举策略
 */
class NoOpLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

  this.logIdent = "[NoOpLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    warn("I should never have been asked to perform leader election, returning the current LeaderAndIsr and replica assignment.")
    (currentLeaderAndIsr, controllerContext.partitionReplicaAssignment(topicAndPartition))
  }
}
