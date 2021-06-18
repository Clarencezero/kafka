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

package kafka.coordinator.group

import java.util

import kafka.utils.nonthreadsafe

/**
 * 组成员概要数据，提取了最核心的元数据信息
 *
 * @param memberId          成员唯一ID，由Kakfa自动生成，生成规则：消费组ID-<序号>-
 * @param groupInstanceId   成员静态ID，由客户端配置group.instance.id参数，引入这个参数能够规避不必要的消费者组分区重平衡操作
 * @param clientId          客户端ID，成员配置client.id参数，由于memberId用户不可配置，可以使用此参数作为区分同一个消费者组下的不同成员
 * @param clientHost        客户端主机名，记录这个客户端是从哪台机器发送的消费请求
 * @param metadata          成员所使用的分区分配策略，字节数组，其值是消费者端配置的partition.assignment.strategy，默认是平均分配
 * @param assignment        保存分配给该成员的订阅分区。分区分配是由消费者代表（称为Leader消费者）完成的，整个分区重平衡操作是由 {@link GroupCoordinator} 组协调器协调完成。
 */
case class MemberSummary(memberId: String,
                         groupInstanceId: Option[String],
                         clientId: String,
                         clientHost: String,
                         metadata: Array[Byte],
                         assignment: Array[Byte])

private object MemberMetadata {
  /**
   * 从一组给定的分区分配策略详情中提取出分区分配策略的名称，并将其封装成一个集合对象，
   * 比如有三个消费者，分别配置分配策略是RangeAssignor、RangeAssignor和RoundRobinAssignor，那么这个方法收到这些参数后会返回「RangeAssignor, RoundRobinAssignor」
   * 这个方法常用来统计一个消费者组共配置多少种分区分配策略
   * @param supportedProtocols
   * @return
   */
  def plainProtocolSet(supportedProtocols: List[(String, Array[Byte])]) = supportedProtocols.map(_._1).toSet
}

/**
 * Member metadata contains the following metadata:
 *
 * Heartbeat metadata:
 * 1. negotiated heartbeat session timeout
 * 2. timestamp of the latest heartbeat
 *
 * Protocol metadata:
 * 1. the list of supported protocols (ordered by preference)
 * 2. the metadata associated with each protocol
 *
 * In addition, it also contains the following state information:
 *
 * 1. Awaiting rebalance callback: when the group is in the prepare-rebalance state,
 *                                 its rebalance callback will be kept in the metadata if the
 *                                 member has sent the join group request
 * 2. Awaiting sync callback: when the group is in the awaiting-sync state, its sync callback
 *                            is kept in metadata until the leader provides the group assignment
 *                            and the group transitions to stable
 *
 * @param memberId              消费者成员ID
 * @param groupInstanceId       消费者静态ID
 * @param clientId              客户端ID
 * @param clientHost            客户端IP
 * @param rebalanceTimeoutMs    分区重平衡超时时间
 * @param sessionTimeoutMs      会话超时时间，消费者和组协调器之间依靠心跳机制保活，如果会话超时则组协调器判定该消费者离线，主动触发分区重平衡操作。
 * @param protocolType          协议类型，实际上表示消费者组被用在哪个场景。对于普通的消费者，该值是consumer。对于提供给kakfa connect组件中的消费者而言，该值是connect。
 * @param supportedProtocols    成员所支持的分区分配策略
 * @param assignment            消费者组成员的分区分配方案
 */
@nonthreadsafe
private[group] class MemberMetadata(var memberId: String,
                                    val groupInstanceId: Option[String],
                                    val clientId: String,
                                    val clientHost: String,
                                    val rebalanceTimeoutMs: Int,
                                    val sessionTimeoutMs: Int,
                                    val protocolType: String,
                                    var supportedProtocols: List[(String, Array[Byte])],
                                    var assignment: Array[Byte] = Array.empty[Byte]) {

  /**
   * 组成员是否正在等待加入消费者组
   */
  var awaitingJoinCallback: JoinGroupResult => Unit = null

  /**
   * 组成员是否正在等待组协调器发送分配方案（消费者组成员处于同步状态）
   */
  var awaitingSyncCallback: SyncGroupResult => Unit = null

  /**
   * 组成员是否发起"退出组"操作
   */
  var isLeaving: Boolean = false

  /**
   * 是否为消费者组的新成员
   */
  var isNew: Boolean = false

  /**
   * 组成员是否为静态成员
   */
  def isStaticMember: Boolean = groupInstanceId.isDefined

  // This variable is used to track heartbeat completion through the delayed
  // heartbeat purgatory. When scheduling a new heartbeat expiration, we set
  // this value to `false`. Upon receiving the heartbeat (or any other event
  // indicating the liveness of the client), we set it to `true` so that the
  // delayed heartbeat can be completed.
  /**
   * 消费者和组协调器连接成功，建立心跳保活机制
   * 组协调器->将消费者的心跳检查封装成延迟任务放入到heartbeat purgatory中，延迟时间为sessionTimeoutMs
   * 经过sessionTimeoutMs时间后触发延迟任务执行->任务检查此标志是否为true，如果为true，说明消费者心跳保活机制正常，更新该值为false，然后再将新的心跳检查任务封装为延迟任务放入heartbeat purgatory中。
   *
   * 消费者心跳保活->向组协调器发送心跳->组协调器收到心跳后更新此值为true
   */
  var heartbeatSatisfied: Boolean = false

  def isAwaitingJoin = awaitingJoinCallback != null
  def isAwaitingSync = awaitingSyncCallback != null

  /**
   * 从配置的分区分配策略中寻找给定的策略
   * @param protocol
   * @return
   */
  def metadata(protocol: String): Array[Byte] = {
    supportedProtocols.find(_._1 == protocol) match {
      case Some((_, metadata)) => metadata
      case None =>
        throw new IllegalArgumentException("Member does not support protocol")
    }
  }

  def hasSatisfiedHeartbeat: Boolean = {
    if (isNew) {
      // New members can be expired while awaiting join, so we have to check this first
      heartbeatSatisfied
    } else if (isAwaitingJoin || isAwaitingSync) {
      // Members that are awaiting a rebalance automatically satisfy expected heartbeats
      true
    } else {
      // Otherwise we require the next heartbeat
      heartbeatSatisfied
    }
  }

  /**
   * Check if the provided protocol metadata matches the currently stored metadata.
   */
  def matches(protocols: List[(String, Array[Byte])]): Boolean = {
    if (protocols.size != this.supportedProtocols.size)
      return false

    for (i <- protocols.indices) {
      val p1 = protocols(i)
      val p2 = supportedProtocols(i)
      if (p1._1 != p2._1 || !util.Arrays.equals(p1._2, p2._2))
        return false
    }
    true
  }

  def summary(protocol: String): MemberSummary = {
    MemberSummary(memberId, groupInstanceId, clientId, clientHost, metadata(protocol), assignment)
  }

  def summaryNoMetadata(): MemberSummary = {
    MemberSummary(memberId, groupInstanceId, clientId, clientHost, Array.empty[Byte], Array.empty[Byte])
  }

  /**
   * Vote for one of the potential group protocols. This takes into account the protocol preference as
   * indicated by the order of supported protocols and returns the first one also contained in the set
   */
  def vote(candidates: Set[String]): String = {
    supportedProtocols.find({ case (protocol, _) => candidates.contains(protocol)}) match {
      case Some((protocol, _)) => protocol
      case None =>
        throw new IllegalArgumentException("Member does not support any of the candidate protocols")
    }
  }

  override def toString: String = {
    "MemberMetadata(" +
      s"memberId=$memberId, " +
      s"groupInstanceId=$groupInstanceId, " +
      s"clientId=$clientId, " +
      s"clientHost=$clientHost, " +
      s"sessionTimeoutMs=$sessionTimeoutMs, " +
      s"rebalanceTimeoutMs=$rebalanceTimeoutMs, " +
      s"supportedProtocols=${supportedProtocols.map(_._1)}, " +
      ")"
  }
}
