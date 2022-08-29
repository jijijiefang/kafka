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

package kafka.coordinator

import kafka.server.DelayedOperation

/**
 * Delayed rebalance operations that are added to the purgatory when group is preparing for rebalance
 * 当组准备重新平衡时添加到炼狱的延迟重新平衡操作
 * Whenever a join-group request is received, check if all known group members have requested
 * to re-join the group; if yes, complete this operation to proceed rebalance.
 * 每当收到加入群组的请求时，检查是否所有已知的群组成员都请求重新加入群组；如果是，请完成此操作以进行重新平衡。
 * When the operation has expired, any known members that have not requested to re-join
 * the group are marked as failed, and complete this operation to proceed rebalance with
 * the rest of the group.
 * 当操作到期时，任何尚未请求重新加入组的已知成员都被标记为失败，并完成此操作以与组的其余部分进行重新平衡。
 */
private[coordinator] class DelayedJoin(coordinator: GroupCoordinator,
                                            group: GroupMetadata,
                                            sessionTimeout: Long)
  extends DelayedOperation(sessionTimeout) {

  override def tryComplete(): Boolean = coordinator.tryCompleteJoin(group, forceComplete)
  override def onExpiration() = coordinator.onExpireJoin()
  override def onComplete() = coordinator.onCompleteJoin(group)
}
