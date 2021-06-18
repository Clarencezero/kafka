/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import kafka.common.OffsetAndMetadata
import kafka.utils.{CoreUtils, Logging, nonthreadsafe}
import kafka.utils.Implicits._
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.Time

import scala.collection.{Seq, immutable, mutable}
import scala.jdk.CollectionConverters._

/**
 * 定义消费者组状态，包括
 * {@link Empty} 无成员但注册信息尚未过期
 * {@link PreparingRebalance} 等待执行「分区重平衡」操作
 * {@link CompletingRebalance} 等待消费者代表（leader）发送已分配好的分区分配方案
 * {@link Stable} 消费者组达到平衡的状态，即正常工作的状态
 * {@link Dead} 无成员且注册数据等待被删除
 */
private[group] sealed trait GroupState {
  /**
   * 合法的前序状态
   */
  val validPreviousStates: Set[GroupState]
}

/**
 * Group is preparing to rebalance
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 * respond to sync group with REBALANCE_IN_PROGRESS
 * remove member on leave group request
 * park join group requests from new or existing members until all expected members have joined
 * allow offset commits from previous generation
 * allow offset fetch requests
 * transition: some members have joined by the timeout => CompletingRebalance
 * all members have left the group => Empty
 * group is removed by partition emigration => Dead
 */
private[group] case object PreparingRebalance extends GroupState {
  val validPreviousStates: Set[GroupState] = Set(Stable, CompletingRebalance, Empty)
}

/**
 * Group is awaiting state assignment from the leader
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 * respond to offset commits with REBALANCE_IN_PROGRESS
 * park sync group requests from followers until transition to Stable
 * allow offset fetch requests
 * transition: sync group with state assignment received from leader => Stable
 * join group from new member or existing member with updated metadata => PreparingRebalance
 * leave group from existing member => PreparingRebalance
 * member failure detected => PreparingRebalance
 * group is removed by partition emigration => Dead
 */
private[group] case object CompletingRebalance extends GroupState {
  val validPreviousStates: Set[GroupState] = Set(PreparingRebalance)
}

/**
 * Group is stable
 *
 * action: respond to member heartbeats normally
 * respond to sync group from any member with current assignment
 * respond to join group from followers with matching metadata with current group metadata
 * allow offset commits from member of current generation
 * allow offset fetch requests
 * transition: member failure detected via heartbeat => PreparingRebalance
 * leave group from existing member => PreparingRebalance
 * leader join-group received => PreparingRebalance
 * follower join-group with new metadata => PreparingRebalance
 * group is removed by partition emigration => Dead
 */
private[group] case object Stable extends GroupState {
  val validPreviousStates: Set[GroupState] = Set(CompletingRebalance)
}

/**
 * Group has no more members and its metadata is being removed
 *
 * action: respond to join group with UNKNOWN_MEMBER_ID
 * respond to sync group with UNKNOWN_MEMBER_ID
 * respond to heartbeat with UNKNOWN_MEMBER_ID
 * respond to leave group with UNKNOWN_MEMBER_ID
 * respond to offset commit with UNKNOWN_MEMBER_ID
 * allow offset fetch requests
 * transition: Dead is a final state before group metadata is cleaned up, so there are no transitions
 */
private[group] case object Dead extends GroupState {
  val validPreviousStates: Set[GroupState] = Set(Stable, PreparingRebalance, CompletingRebalance, Empty, Dead)
}

/**
 * Group has no more members, but lingers until all offsets have expired. This state
 * also represents groups which use Kafka only for offset commits and have no members.
 *
 * action: respond normally to join group from new members
 * respond to sync group with UNKNOWN_MEMBER_ID
 * respond to heartbeat with UNKNOWN_MEMBER_ID
 * respond to leave group with UNKNOWN_MEMBER_ID
 * respond to offset commit with UNKNOWN_MEMBER_ID
 * allow offset fetch requests
 * transition: last offsets removed in periodic expiration task => Dead
 * join group from a new member => PreparingRebalance
 * group is removed by partition emigration => Dead
 * group is removed by expiration => Dead
 */
private[group] case object Empty extends GroupState {
  val validPreviousStates: Set[GroupState] = Set(PreparingRebalance)
}

/**
 * 消费者组元数据
 */
private object GroupMetadata extends Logging {

  /**
   *
   * @param groupId      消费者组ID
   * @param initialState 消费者组初始状态
   * @param generationId
   * @param protocolType 消费者组协议类型
   * @param protocolName 消费者组协议名称
   * @param leaderId     消费者成员代表ID
   * @param currentStateTimestamp
   * @param members      消费者组组员元数据
   * @param time         时间工具类
   * @return
   */
  def loadGroup(groupId: String,
                initialState: GroupState,
                generationId: Int,
                protocolType: String,
                protocolName: String,
                leaderId: String,
                currentStateTimestamp: Option[Long],
                members: Iterable[MemberMetadata],
                time: Time): GroupMetadata = {
    val group = new GroupMetadata(groupId, initialState, time)
    group.generationId = generationId
    group.protocolType = if (protocolType == null || protocolType.isEmpty) None else Some(protocolType)
    group.protocolName = Option(protocolName)
    group.leaderId = Option(leaderId)
    group.currentStateTimestamp = currentStateTimestamp
    members.foreach { member =>
      group.add(member, null)
      if (member.isStaticMember) {
        group.addStaticMember(member.groupInstanceId, member.memberId)
      }
      info(s"Loaded member $member in group $groupId with generation ${group.generationId}.")
    }
    group.subscribedTopics = group.computeSubscribedTopics()
    group
  }

  private val MemberIdDelimiter = "-"
}

/**
 * POJO类，用于表示消费者组元数据信息，当我们使用命令kafka-consumer-group.sh --list时就会使用这个对象封装消费组元数据并返回
 *
 * @param groupId      消费组ID
 * @param protocolType 消费者组的协议类型
 * @param state        消费者组状态
 */
case class GroupOverview(groupId: String,
                         protocolType: String,
                         state: String)

/**
 * Case class used to represent group metadata for the DescribeGroup API
 */

/**
 * POJO类，比 {@link GroupOverview} 存储的内容稍微多一点
 *
 * @param state        消费者组状态
 * @param protocolType 消费者组的协议类型
 * @param protocol     消费者组选定的分区分配策略
 * @param members      消费者组成员元数据概要
 */
case class GroupSummary(state: String,
                        protocolType: String,
                        protocol: String,
                        members: List[MemberSummary])

/**
 * We cache offset commits along with their commit record offset. This enables us to ensure that the latest offset
 * commit is always materialized when we have a mix of transactional and regular offset commits. Without preserving
 * information of the commit record offset, compaction of the offsets topic itself may result in the wrong offset commit
 * being materialized.
 */

/**
 * 保存位移提交消息的位移值及其它元数据
 *
 * @param appendedBatchOffset 位移主题消息自己的位移值，即向__consumer_offset哪个位置写入下面的消息
 * @param offsetAndMetadata   位移提交消息中保存的消息者组的位移值
 */
case class CommitRecordMetadataAndOffset(appendedBatchOffset: Option[Long], offsetAndMetadata: OffsetAndMetadata) {
  def olderThan(that: CommitRecordMetadataAndOffset): Boolean = appendedBatchOffset.get < that.appendedBatchOffset.get
}

/**
 * Group contains the following metadata:
 *
 * Membership metadata:
 *  1. Members registered in this group
 *     2. Current protocol assigned to the group (e.g. partition assignment strategy for consumers)
 *     3. Protocol metadata associated with group members
 *
 * State metadata:
 *  1. group state
 *     2. generation id
 *     3. leader id
 *
 *
 * 消费者组元数据，比如当前消费者组状态、消费者组有哪些消费者成员组成，u
 * 所以一个 {@link GroupMetadata} 会包含多个消费者成员信息 {@link MemberMetadata}
 * 可以从以下4个角度理解消费者组元数据：消费者组状态、成员、位移和分区分配策略
 *
 * @param groupId      消费者组ID
 * @param initialState 消费者组状态
 * @param time         时间工具类
 */
@nonthreadsafe
private[group] class GroupMetadata(val groupId: String, initialState: GroupState, time: Time) extends Logging {
  type JoinCallback = JoinGroupResult => Unit

  private[group] val lock = new ReentrantLock

  // 消费者组状态
  private var state: GroupState = initialState
  /**
   * 最近一次状态变更的时间戳，用于确定位移主题中的过期消息。
   * 位移主题中的消息也要遵循Kafka的留存策略，所有当前时间与该字段的差值超过了留存阈值的消息都会被视作「已过期（Expired）」
   */
  var currentStateTimestamp: Option[Long] = Some(time.milliseconds())
  /**
   * 协议类型
   */
  var protocolType: Option[String] = None

  /**
   * 协议名称
   */
  var protocolName: Option[String] = None
  /**
   * 消费组Generation号，等同于消费者组执行过分区重平衡操作的次数，
   * 每次执行都会+1
   */
  var generationId = 0

  /**
   * 消费者代表的成员ID号，它主要用来根据分区分配策略执行分区分配任务，
   * 然后把得到的结果发送给组协调器，再由组协调器广播给其它组成员
   */
  private var leaderId: Option[String] = None

  /**
   * 消费组中消费者的元数据信息
   */
  private val members = new mutable.HashMap[String, MemberMetadata]

  // 拥有静态ID的消费者<group.instance.id, member.id>
  private val staticMembers = new mutable.HashMap[String, String]
  private val pendingMembers = new mutable.HashSet[String]
  /**
   * 成员等待加入消费者组的数量
   */
  private var numMembersAwaitingJoin = 0

  /**
   * 消费者成员所支持分配分配策略的票数
   * 组协调器会统计消费组组员提供的分区分配策略，选取票数最多的分区分配策略作为当前消费组的默认分区分配算法
   */
  private val supportedProtocols = new mutable.HashMap[String, Integer]().withDefaultValue(0)
  /**
   * 保存消费者组订阅分区的提交位移值，key：分区,value：消费者所提交的消费位移
   * 区分「位移」和「位移提交消息」
   * 消费者需要向组协调器（GroupCoordinator）提交已消费消息的进度，称作「提交位移」。Kafka会将该值用于定位消费者组要消费的下一条消息。
   * 提交的位移信息是保存在主题中，即__consumer_offset，消费者向内部主题写入特定格式的事件消息，即所谓的Commit Record。
   *
   */
  private val offsets = new mutable.HashMap[TopicPartition, CommitRecordMetadataAndOffset]
  private val pendingOffsetCommits = new mutable.HashMap[TopicPartition, OffsetAndMetadata]
  private val pendingTransactionalOffsetCommits = new mutable.HashMap[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]()
  private var receivedTransactionalOffsetCommits = false
  private var receivedConsumerOffsetCommits = false

  /**
   * 当 protocolType == `consumer` 时，需要维护成员所订阅的主题列表
   */
  private var subscribedTopics: Option[Set[String]] = None

  var newMemberAdded: Boolean = false

  def inLock[T](fun: => T): T = CoreUtils.inLock(lock)(fun)

  def is(groupState: GroupState) = state == groupState

  def not(groupState: GroupState) = state != groupState

  def has(memberId: String) = members.contains(memberId)

  def get(memberId: String) = members(memberId)

  def size = members.size

  def isLeader(memberId: String): Boolean = leaderId.contains(memberId)

  def leaderOrNull: String = leaderId.orNull

  /**
   *
   * @return
   */
  def currentStateTimestampOrDefault: Long = currentStateTimestamp.getOrElse(-1)

  def isConsumerGroup: Boolean = protocolType.contains(ConsumerProtocol.PROTOCOL_TYPE)

  /**
   * 添加一个消费者组员
   * 1.校验
   * 2.更新leader
   * 3.更新members
   * 4.更新分区分配策略支持票数
   * 5.设置成员的回调函数
   * 6.更新已加入组的成员数量
   *
   * @param member   新增的消费者
   * @param callback 回调方法
   */
  def add(member: MemberMetadata, callback: JoinCallback = null): Unit = {
    // 首位成员
    if (members.isEmpty) {
      // 就把该成员的protocolType设置为消费者组的protocolType
      // 普通的消费者protocolType=consumer
      this.protocolType = Some(member.protocolType)
    }

    assert(this.protocolType.orNull == member.protocolType)
    assert(supportsProtocols(member.protocolType, MemberMetadata.plainProtocolSet(member.supportedProtocols)))

    // 此时消费者组还没有选出消费者代表（leader）
    if (leaderId.isEmpty) {
      // 把刚加入的消费者定为消费者代表
      leaderId = Some(member.memberId)
    }
    // 将该成员添加进members
    members.put(member.memberId, member)
    // 更新分区分配策略的支持票数
    member.supportedProtocols.foreach { case (protocol, _) => supportedProtocols(protocol) += 1 }
    // 设置成员加入组后的回调函数
    member.awaitingJoinCallback = callback
    // 更新已加入组的成员数量
    if (member.isAwaitingJoin)
      numMembersAwaitingJoin += 1
  }

  /**
   * 移除消费者
   *
   * @param memberId 待移除的消费者成员ID
   */
  def remove(memberId: String): Unit = {
    // 从members移除成员ID
    members.remove(memberId).foreach { member =>
      // 更新分区分配策略的支持票数
      member.supportedProtocols.foreach { case (protocol, _) => supportedProtocols(protocol) -= 1 }
      // 如果成员此刻等待加入消费者组，则更新numMembersAwaitingJoin
      if (member.isAwaitingJoin)
        numMembersAwaitingJoin -= 1
      // 如果成员有静态ID，则移除
      member.groupInstanceId.foreach(staticMembers.remove)
    }

    // 如果成员是消费者代表，则需要从成员列表中选择下一个成员作为leader
    if (isLeader(memberId))
      leaderId = members.keys.headOption
  }

  /**
   * Check whether current leader is rejoined. If not, try to find another joined member to be
   * new leader. Return false if
   *   1. the group is currently empty (has no designated leader)
   *      2. no member rejoined
   */
  def maybeElectNewJoinedLeader(): Boolean = {
    leaderId.exists { currentLeaderId =>
      val currentLeader = get(currentLeaderId)
      if (!currentLeader.isAwaitingJoin) {
        members.find(_._2.isAwaitingJoin) match {
          case Some((anyJoinedMemberId, anyJoinedMember)) =>
            leaderId = Option(anyJoinedMemberId)
            info(s"Group leader [member.id: ${currentLeader.memberId}, " +
              s"group.instance.id: ${currentLeader.groupInstanceId}] failed to join " +
              s"before rebalance timeout, while new leader $anyJoinedMember was elected.")
            true

          case None =>
            info(s"Group leader [member.id: ${currentLeader.memberId}, " +
              s"group.instance.id: ${currentLeader.groupInstanceId}] failed to join " +
              s"before rebalance timeout, and the group couldn't proceed to next generation" +
              s"because no member joined.")
            false
        }
      } else {
        true
      }
    }
  }

  /**
   * [For static members only]: Replace the old member id with the new one,
   * keep everything else unchanged and return the updated member.
   */
  def replaceGroupInstance(oldMemberId: String,
                           newMemberId: String,
                           groupInstanceId: Option[String]): MemberMetadata = {
    if (groupInstanceId.isEmpty) {
      throw new IllegalArgumentException(s"unexpected null group.instance.id in replaceGroupInstance")
    }
    val oldMember = members.remove(oldMemberId)
      .getOrElse(throw new IllegalArgumentException(s"Cannot replace non-existing member id $oldMemberId"))

    // Fence potential duplicate member immediately if someone awaits join/sync callback.
    maybeInvokeJoinCallback(oldMember, JoinGroupResult(oldMemberId, Errors.FENCED_INSTANCE_ID))

    maybeInvokeSyncCallback(oldMember, SyncGroupResult(Errors.FENCED_INSTANCE_ID))

    oldMember.memberId = newMemberId
    members.put(newMemberId, oldMember)

    if (isLeader(oldMemberId))
      leaderId = Some(newMemberId)
    addStaticMember(groupInstanceId, newMemberId)
    oldMember
  }

  def isPendingMember(memberId: String): Boolean = pendingMembers.contains(memberId) && !has(memberId)

  def addPendingMember(memberId: String) = pendingMembers.add(memberId)

  def removePendingMember(memberId: String) = pendingMembers.remove(memberId)

  def hasStaticMember(groupInstanceId: Option[String]) = groupInstanceId.isDefined && staticMembers.contains(groupInstanceId.get)

  def getStaticMemberId(groupInstanceId: Option[String]) = {
    if (groupInstanceId.isEmpty) {
      throw new IllegalArgumentException(s"unexpected null group.instance.id in getStaticMemberId")
    }
    staticMembers(groupInstanceId.get)
  }

  def addStaticMember(groupInstanceId: Option[String], newMemberId: String) = {
    if (groupInstanceId.isEmpty) {
      throw new IllegalArgumentException(s"unexpected null group.instance.id in addStaticMember")
    }
    staticMembers.put(groupInstanceId.get, newMemberId)
  }

  def currentState = state

  def notYetRejoinedMembers = members.filter(!_._2.isAwaitingJoin).toMap

  def hasAllMembersJoined = members.size == numMembersAwaitingJoin && pendingMembers.isEmpty

  def allMembers = members.keySet

  def allStaticMembers = staticMembers.keySet

  // For testing only.
  def allDynamicMembers = {
    val dynamicMemberSet = new mutable.HashSet[String]
    allMembers.foreach(memberId => dynamicMemberSet.add(memberId))
    staticMembers.values.foreach(memberId => dynamicMemberSet.remove(memberId))
    dynamicMemberSet.toSet
  }

  def numPending = pendingMembers.size

  def numAwaiting: Int = numMembersAwaitingJoin

  def allMemberMetadata = members.values.toList

  def rebalanceTimeoutMs = members.values.foldLeft(0) { (timeout, member) =>
    timeout.max(member.rebalanceTimeoutMs)
  }

  def generateMemberId(clientId: String,
                       groupInstanceId: Option[String]): String = {
    groupInstanceId match {
      case None =>
        clientId + GroupMetadata.MemberIdDelimiter + UUID.randomUUID().toString
      case Some(instanceId) =>
        instanceId + GroupMetadata.MemberIdDelimiter + UUID.randomUUID().toString
    }
  }

  /**
   * Verify the member.id is up to date for static members. Return true if both conditions met:
   *   1. given member is a known static member to group
   *      2. group stored member.id doesn't match with given member.id
   */
  def isStaticMemberFenced(memberId: String,
                           groupInstanceId: Option[String],
                           operation: String): Boolean = {
    if (hasStaticMember(groupInstanceId)
      && getStaticMemberId(groupInstanceId) != memberId) {
      error(s"given member.id $memberId is identified as a known static member ${groupInstanceId.get}, " +
        s"but not matching the expected member.id ${getStaticMemberId(groupInstanceId)} during $operation, will " +
        s"respond with instance fenced error")
      true
    } else
      false
  }

  /**
   * 判断消费者组是否能够开启「Rebalance」操作，依据是：当前状态是否是 RreparingRebalance 的合法的前序状态
   * 只有 Stable、CompletingRebalance和Empty这3类状态才有资格开启Rebalance操作
   */
  def canRebalance = PreparingRebalance.validPreviousStates.contains(state)

  /**
   * 将消费者组状态设置为目标状态
   *
   * @param groupState 目标状态
   */
  def transitionTo(groupState: GroupState): Unit = {
    // #1 判断前序状态是否合法
    assertValidTransition(groupState)

    // #2 更改状态值
    state = groupState

    // #3 更新时间戳，方便定时任务定期清过期的消费者组位移数据
    currentStateTimestamp = Some(time.milliseconds())
  }

  def selectProtocol: String = {
    if (members.isEmpty)
      throw new IllegalStateException("Cannot select protocol for empty group")

    // select the protocol for this group which is supported by all members
    val candidates = candidateProtocols

    // let each member vote for one of the protocols and choose the one with the most votes
    val (protocol, _) = allMemberMetadata
      .map(_.vote(candidates))
      .groupBy(identity)
      .maxBy { case (_, votes) => votes.size }

    protocol
  }

  /**
   * 获取组内所有成员都支持的分区分配策略集合
   * @return
   */
  private def candidateProtocols: Set[String] = {
    // get the set of protocols that are commonly supported by all members
    val numMembers = members.size
    supportedProtocols.filter(_._2 == numMembers).map(_._1).toSet
  }

  def supportsProtocols(memberProtocolType: String, memberProtocols: Set[String]): Boolean = {
    if (is(Empty))
      !memberProtocolType.isEmpty && memberProtocols.nonEmpty
    else
      protocolType.contains(memberProtocolType) && memberProtocols.exists(supportedProtocols(_) == members.size)
  }

  def getSubscribedTopics: Option[Set[String]] = subscribedTopics

  /**
   * Returns true if the consumer group is actively subscribed to the topic. When the consumer
   * group does not know, because the information is not available yet or because the it has
   * failed to parse the Consumer Protocol, it returns true to be safe.
   */
  def isSubscribedToTopic(topic: String): Boolean = subscribedTopics match {
    case Some(topics) => topics.contains(topic)
    case None => true
  }

  /**
   * Collects the set of topics that the members are subscribed to when the Protocol Type is equal
   * to 'consumer'. None is returned if
   * - the protocol type is not equal to 'consumer';
   * - the protocol is not defined yet; or
   * - the protocol metadata does not comply with the schema.
   */
  private[group] def computeSubscribedTopics(): Option[Set[String]] = {
    protocolType match {
      case Some(ConsumerProtocol.PROTOCOL_TYPE) if members.nonEmpty && protocolName.isDefined =>
        try {
          Some(
            members.map { case (_, member) =>
              // The consumer protocol is parsed with V0 which is the based prefix of all versions.
              // This way the consumer group manager does not depend on any specific existing or
              // future versions of the consumer protocol. VO must prefix all new versions.
              val buffer = ByteBuffer.wrap(member.metadata(protocolName.get))
              ConsumerProtocol.deserializeVersion(buffer)
              ConsumerProtocol.deserializeSubscription(buffer, 0).topics.asScala.toSet
            }.reduceLeft(_ ++ _)
          )
        } catch {
          case e: SchemaException =>
            warn(s"Failed to parse Consumer Protocol ${ConsumerProtocol.PROTOCOL_TYPE}:${protocolName.get} " +
              s"of group $groupId. Consumer group coordinator is not aware of the subscribed topics.", e)
            None
        }

      case Some(ConsumerProtocol.PROTOCOL_TYPE) if members.isEmpty =>
        Option(Set.empty)

      case _ => None
    }
  }

  def updateMember(member: MemberMetadata,
                   protocols: List[(String, Array[Byte])],
                   callback: JoinCallback): Unit = {
    member.supportedProtocols.foreach { case (protocol, _) => supportedProtocols(protocol) -= 1 }
    protocols.foreach { case (protocol, _) => supportedProtocols(protocol) += 1 }
    member.supportedProtocols = protocols

    if (callback != null && !member.isAwaitingJoin) {
      numMembersAwaitingJoin += 1
    } else if (callback == null && member.isAwaitingJoin) {
      numMembersAwaitingJoin -= 1
    }
    member.awaitingJoinCallback = callback
  }

  def maybeInvokeJoinCallback(member: MemberMetadata,
                              joinGroupResult: JoinGroupResult): Unit = {
    if (member.isAwaitingJoin) {
      member.awaitingJoinCallback(joinGroupResult)
      member.awaitingJoinCallback = null
      numMembersAwaitingJoin -= 1
    }
  }

  /**
   * @return true if a sync callback actually performs.
   */
  def maybeInvokeSyncCallback(member: MemberMetadata,
                              syncGroupResult: SyncGroupResult): Boolean = {
    if (member.isAwaitingSync) {
      member.awaitingSyncCallback(syncGroupResult)
      member.awaitingSyncCallback = null
      true
    } else {
      false
    }
  }

  def initNextGeneration(): Unit = {
    if (members.nonEmpty) {
      generationId += 1
      protocolName = Some(selectProtocol)
      subscribedTopics = computeSubscribedTopics()
      transitionTo(CompletingRebalance)
    } else {
      generationId += 1
      protocolName = None
      subscribedTopics = computeSubscribedTopics()
      transitionTo(Empty)
    }
    receivedConsumerOffsetCommits = false
    receivedTransactionalOffsetCommits = false
  }

  def currentMemberMetadata: List[JoinGroupResponseMember] = {
    if (is(Dead) || is(PreparingRebalance))
      throw new IllegalStateException("Cannot obtain member metadata for group in state %s".format(state))
    members.map { case (memberId, memberMetadata) => new JoinGroupResponseMember()
      .setMemberId(memberId)
      .setGroupInstanceId(memberMetadata.groupInstanceId.orNull)
      .setMetadata(memberMetadata.metadata(protocolName.get))
    }.toList
  }

  def summary: GroupSummary = {
    if (is(Stable)) {
      val protocol = protocolName.orNull
      if (protocol == null)
        throw new IllegalStateException("Invalid null group protocol for stable group")

      val members = this.members.values.map { member => member.summary(protocol) }
      GroupSummary(state.toString, protocolType.getOrElse(""), protocol, members.toList)
    } else {
      val members = this.members.values.map { member => member.summaryNoMetadata() }
      GroupSummary(state.toString, protocolType.getOrElse(""), GroupCoordinator.NoProtocol, members.toList)
    }
  }

  def overview: GroupOverview = {
    GroupOverview(groupId, protocolType.getOrElse(""), state.toString)
  }

  /**
   * 将给定的一组订阅分区提交位移值添加到 {@link GroupMetadata.offsets} 变量中
   * 同时会更新 {@link GroupMetadata.pendingTransactionalOffsetCommits} 变量，这个变量与Kafka事务相关
   * 当消费者组协调器启动时，它会创建一个异步任务，定期了读取位移评理中相应消息者组的提交位移数据，并将它们加载到offsets字段中
   *
   * @param offsets
   * @param pendingTxnOffsets
   */
  def initializeOffsets(offsets: collection.Map[TopicPartition, CommitRecordMetadataAndOffset],
                        pendingTxnOffsets: Map[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]): Unit = {
    this.offsets ++= offsets
    this.pendingTransactionalOffsetCommits ++= pendingTxnOffsets
  }

  /**
   * 更新offsets缓存数据，这个方法在成功提交位移消息后调用
   *
   * @param topicPartition
   * @param offsetWithCommitRecordMetadata
   */
  def onOffsetCommitAppend(topicPartition: TopicPartition, offsetWithCommitRecordMetadata: CommitRecordMetadataAndOffset): Unit = {
    if (pendingOffsetCommits.contains(topicPartition)) {
      if (offsetWithCommitRecordMetadata.appendedBatchOffset.isEmpty)
        throw new IllegalStateException("Cannot complete offset commit write without providing the metadata of the record " +
          "in the log.")

      if (!offsets.contains(topicPartition) || // 字段offsets中没有包含该分区位移提交数据或
        // 字段offsets中该分区对应的提交位移小于待写入的位移值
        offsets(topicPartition).olderThan(offsetWithCommitRecordMetadata)) {
        // 更新offsets字段中的位移值
        offsets.put(topicPartition, offsetWithCommitRecordMetadata)
      }
    }

    // 事务相关
    pendingOffsetCommits.get(topicPartition) match {
      case Some(stagedOffset) if offsetWithCommitRecordMetadata.offsetAndMetadata == stagedOffset =>
        pendingOffsetCommits.remove(topicPartition)
      case _ =>
      // The pendingOffsetCommits for this partition could be empty if the topic was deleted, in which case
      // its entries would be removed from the cache by the `removeOffsets` method.
    }
  }

  def failPendingOffsetWrite(topicPartition: TopicPartition, offset: OffsetAndMetadata): Unit = {
    pendingOffsetCommits.get(topicPartition) match {
      case Some(pendingOffset) if offset == pendingOffset => pendingOffsetCommits.remove(topicPartition)
      case _ =>
    }
  }

  def prepareOffsetCommit(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {
    receivedConsumerOffsetCommits = true
    pendingOffsetCommits ++= offsets
  }

  def prepareTxnOffsetCommit(producerId: Long, offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {
    trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $offsets is pending")
    receivedTransactionalOffsetCommits = true
    val producerOffsets = pendingTransactionalOffsetCommits.getOrElseUpdate(producerId,
      mutable.Map.empty[TopicPartition, CommitRecordMetadataAndOffset])

    offsets.forKeyValue { (topicPartition, offsetAndMetadata) =>
      producerOffsets.put(topicPartition, CommitRecordMetadataAndOffset(None, offsetAndMetadata))
    }
  }

  def hasReceivedConsistentOffsetCommits: Boolean = {
    !receivedConsumerOffsetCommits || !receivedTransactionalOffsetCommits
  }

  /* Remove a pending transactional offset commit if the actual offset commit record was not written to the log.
   * We will return an error and the client will retry the request, potentially to a different coordinator.
   */
  def failPendingTxnOffsetCommit(producerId: Long, topicPartition: TopicPartition): Unit = {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffsets) =>
        val pendingOffsetCommit = pendingOffsets.remove(topicPartition)
        trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetCommit failed " +
          s"to be appended to the log")
        if (pendingOffsets.isEmpty)
          pendingTransactionalOffsetCommits.remove(producerId)
      case _ =>
      // We may hit this case if the partition in question has emigrated already.
    }
  }

  def onTxnOffsetCommitAppend(producerId: Long, topicPartition: TopicPartition,
                              commitRecordMetadataAndOffset: CommitRecordMetadataAndOffset): Unit = {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffset) =>
        if (pendingOffset.contains(topicPartition)
          && pendingOffset(topicPartition).offsetAndMetadata == commitRecordMetadataAndOffset.offsetAndMetadata)
          pendingOffset.update(topicPartition, commitRecordMetadataAndOffset)
      case _ =>
      // We may hit this case if the partition in question has emigrated.
    }
  }

  /**
   *
   * Complete a pending transactional offset commit. This is called after a commit or abort marker is fully written
   * to the log.
   */

  /**
   * 完成一个待决事务（Pending Transaction）的位移提交
   * Pending Transaction：指正在进行中、还未完成的事务，在处理Pending Transaction的过程中，可能
   * 会出现将Pending Transaction中涉及到的分区的位移值添加到offsets中的情况。
   *
   * @param producerId
   * @param isCommit
   */
  def completePendingTxnOffsetCommit(producerId: Long, isCommit: Boolean): Unit = {
    val pendingOffsetsOpt = pendingTransactionalOffsetCommits.remove(producerId)
    if (isCommit) {
      pendingOffsetsOpt.foreach { pendingOffsets =>
        pendingOffsets.forKeyValue { (topicPartition, commitRecordMetadataAndOffset) =>
          if (commitRecordMetadataAndOffset.appendedBatchOffset.isEmpty)
            throw new IllegalStateException(s"Trying to complete a transactional offset commit for producerId $producerId " +
              s"and groupId $groupId even though the offset commit record itself hasn't been appended to the log.")

          val currentOffsetOpt = offsets.get(topicPartition)
          if (currentOffsetOpt.forall(_.olderThan(commitRecordMetadataAndOffset))) {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              "committed and loaded into the cache.")
            offsets.put(topicPartition, commitRecordMetadataAndOffset)
          } else {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              s"committed, but not loaded since its offset is older than current offset $currentOffsetOpt.")
          }
        }
      }
    } else {
      trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetsOpt aborted")
    }
  }

  def activeProducers: collection.Set[Long] = pendingTransactionalOffsetCommits.keySet

  def hasPendingOffsetCommitsFromProducer(producerId: Long): Boolean =
    pendingTransactionalOffsetCommits.contains(producerId)

  def hasPendingOffsetCommitsForTopicPartition(topicPartition: TopicPartition): Boolean = {
    pendingOffsetCommits.contains(topicPartition) ||
      pendingTransactionalOffsetCommits.exists(
        _._2.contains(topicPartition)
      )
  }

  def removeAllOffsets(): immutable.Map[TopicPartition, OffsetAndMetadata] = removeOffsets(offsets.keySet.toSeq)

  def removeOffsets(topicPartitions: Seq[TopicPartition]): immutable.Map[TopicPartition, OffsetAndMetadata] = {
    topicPartitions.flatMap { topicPartition =>
      pendingOffsetCommits.remove(topicPartition)
      pendingTransactionalOffsetCommits.forKeyValue { (_, pendingOffsets) =>
        pendingOffsets.remove(topicPartition)
      }
      val removedOffset = offsets.remove(topicPartition)
      removedOffset.map(topicPartition -> _.offsetAndMetadata)
    }.toMap
  }

  /**
   * 移除位移值
   * 消费者的位移值也是存储在主题中，所以它也遵循Kafka的留存策略。如果当前时间与提交位移消息时间戳的差值超过Broker端参数
   * offsets.retention.minutes值，kafka就会将这条记录从offsets字段中移除
   *
   * @param currentTimestamp
   * @param offsetRetentionMs
   * @return
   */
  def removeExpiredOffsets(currentTimestamp: Long, offsetRetentionMs: Long): Map[TopicPartition, OffsetAndMetadata] = {

    /**
     * 获取订阅分区过期的位移值
     *
     * @param baseTimestamp    一个函数，接收CommitRecordMetadataAndOffset类型字段，然后计算时间戳并返回
     * @param subscribedTopics 订阅主题集合，默认为空。如果不为空，说明当前主题正被消费者消费，处于正在消费的主题是不能被执行过期位移删除的
     * @return
     */
    def getExpiredOffsets(baseTimestamp: CommitRecordMetadataAndOffset => Long,
                          subscribedTopics: Set[String] = Set.empty): Map[TopicPartition, OffsetAndMetadata] = {
      // 遍历offsets所有分区，过滤出同时满足以下3个条件的所有分区
      // 条件一：分区所属主题不在订阅主题列表之内
      // 条件二：该主题分区已经完成位移提交
      // 条件三：该主题分区在位移主题中对应消息的存在时间超过了阈值
      offsets.filter {
        case (topicPartition, commitRecordMetadataAndOffset) =>
          !subscribedTopics.contains(topicPartition.topic()) && // 不在订阅主题列表中
            !pendingOffsetCommits.contains(topicPartition) && { // 不在待决位移提交中，即该分区已完成位移提交
            commitRecordMetadataAndOffset.offsetAndMetadata.expireTimestamp match {
              // 新版本kafka判断是否过期需要依赖消费者组的状态，如果是Empty，当前时间-变成Empty状态的时间 > expireTime
              // 如果非Empty，当前时间-提交位移消息中的时间>expireTime
              // 如果超过了，就视作过期，对应的位移值需要被移除
              case None =>
                // current version with no per partition retention
                currentTimestamp - baseTimestamp(commitRecordMetadataAndOffset) >= offsetRetentionMs
              case Some(expireTimestamp) =>
                // 分区在「__consumer_offsets」保存的时间是否超过过期阈值
                currentTimestamp >= expireTimestamp
            }
          }
      }.map {
        // 为已过期的分区提取出commitRecordOffsetAndMetadata中的位移值
        case (topicPartition, commitRecordOffsetAndMetadata) =>
          (topicPartition, commitRecordOffsetAndMetadata.offsetAndMetadata)
      }.toMap
    }

    /**
     *
     */
    val expiredOffsets: Map[TopicPartition, OffsetAndMetadata] = protocolType match {
      case Some(_) if is(Empty) =>
        // no consumer exists in the group =>
        // - if current state timestamp exists and retention period has passed since group became Empty,
        //   expire all offsets with no pending offset commit;
        // - if there is no current state timestamp (old group metadata schema) and retention period has passed
        //   since the last commit timestamp, expire the offset
        getExpiredOffsets(
          commitRecordMetadataAndOffset => currentStateTimestamp
            .getOrElse(commitRecordMetadataAndOffset.offsetAndMetadata.commitTimestamp)
        )

      case Some(ConsumerProtocol.PROTOCOL_TYPE) if subscribedTopics.isDefined =>
        // consumers exist in the group =>
        // - if the group is aware of the subscribed topics and retention period had passed since the
        //   the last commit timestamp, expire the offset. offset with pending offset commit are not
        //   expired
        getExpiredOffsets(
          _.offsetAndMetadata.commitTimestamp,
          subscribedTopics.get
        )

      case None =>
        // protocolType is None => standalone (simple) consumer, that uses Kafka for offset storage only
        // expire offsets with no pending offset commit that retention period has passed since their last commit
        getExpiredOffsets(_.offsetAndMetadata.commitTimestamp)

      case _ =>
        Map()
    }

    if (expiredOffsets.nonEmpty)
      debug(s"Expired offsets from group '$groupId': ${expiredOffsets.keySet}")

    // 将过期位移对应的主题分区从offsets中移除
    offsets --= expiredOffsets.keySet
    // 返回主题分区对应的过期位移
    expiredOffsets
  }

  def allOffsets = offsets.map { case (topicPartition, commitRecordMetadataAndOffset) =>
    (topicPartition, commitRecordMetadataAndOffset.offsetAndMetadata)
  }.toMap

  def offset(topicPartition: TopicPartition): Option[OffsetAndMetadata] = offsets.get(topicPartition).map(_.offsetAndMetadata)

  // visible for testing
  private[group] def offsetWithRecordMetadata(topicPartition: TopicPartition): Option[CommitRecordMetadataAndOffset] = offsets.get(topicPartition)

  def numOffsets = offsets.size

  def hasOffsets = offsets.nonEmpty || pendingOffsetCommits.nonEmpty || pendingTransactionalOffsetCommits.nonEmpty

  private def assertValidTransition(targetState: GroupState): Unit = {
    if (!targetState.validPreviousStates.contains(state))
      throw new IllegalStateException("Group %s should be in the %s states before moving to %s state. Instead it is in %s state"
        .format(groupId, targetState.validPreviousStates.mkString(","), targetState, state))
  }

  override def toString: String = {
    "GroupMetadata(" +
      s"groupId=$groupId, " +
      s"generation=$generationId, " +
      s"protocolType=$protocolType, " +
      s"currentState=$currentState, " +
      s"members=$members)"
  }

}

