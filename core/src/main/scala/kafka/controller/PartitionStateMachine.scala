/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.controller

import kafka.api.LeaderAndIsr
import kafka.common.StateChangeFailedException
import kafka.controller.Election._
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.TopicPartitionStateZNode
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code
import scala.collection.{Map, Seq, mutable}

abstract class PartitionStateMachine(controllerContext: ControllerContext) extends Logging {
  /**
   * Invoked on successful controller election.
   */
  def startup(): Unit = {
    info("Initializing partition state")
    initializePartitionState()
    info("Triggering online partition state changes")
    triggerOnlinePartitionStateChange()
    debug(s"Started partition state machine with initial state -> ${controllerContext.partitionStates}")
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown(): Unit = {
    info("Stopped partition state machine")
  }

  /**
   * 此 API 对处于 NewPartition 或 OfflinePartition 状态的所有分区调用 OnlinePartition 状态更改。 这是在成功的控制器选举和代理更改时调用的
   * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
   * state. This is called on a successful controller election and on broker changes
   */
  def triggerOnlinePartitionStateChange(): Unit = {
    // 获取离线分区和新分区
    val partitions = controllerContext.partitionsInStates(Set(OfflinePartition, NewPartition))

    // 尝试将离线分区和新分区的状态修改为「在线」，其中可能触发分区Leader副本选举操作
    // 主要目标是让这些离线或新加入的分区对外提供服务
    triggerOnlineStateChangeForPartitions(partitions)
  }

  def triggerOnlinePartitionStateChange(topic: String): Unit = {
    val partitions = controllerContext.partitionsInStates(topic, Set(OfflinePartition, NewPartition))
    triggerOnlineStateChangeForPartitions(partitions)
  }

  /**
   *
   * @param partitions
   */
  private def triggerOnlineStateChangeForPartitions(partitions: collection.Set[TopicPartition]): Unit = {
    // 剔除主题已被删除的分区，
    val partitionsToTrigger = partitions.filter { partition =>
      !controllerContext.isTopicQueuedUpForDeletion(partition.topic)
    }.toSeq

    // 将这些分区的状态变更为「OnlinePartition」，对于处于离线状态的分区使用
    // OfflinePartitionLeaderElectionStrategy 选举策略选出一个新的Leader副本
    // 并更新ZK节点
    handleStateChanges(partitionsToTrigger, OnlinePartition, Some(OfflinePartitionLeaderElectionStrategy(false)))
    // TODO: If handleStateChanges catches an exception, it is not enough to bail out and log an error.
    // It is important to trigger leader election for those partitions.
  }

  /**
   * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
   * zookeeper
   */
  private def initializePartitionState(): Unit = {
    for (topicPartition <- controllerContext.allPartitions) {
      // check if leader and isr path exists for partition. If not, then it is in NEW state
      controllerContext.partitionLeadershipInfo(topicPartition) match {
        case Some(currentLeaderIsrAndEpoch) =>
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
          if (controllerContext.isReplicaOnline(currentLeaderIsrAndEpoch.leaderAndIsr.leader, topicPartition))
          // leader is alive
            controllerContext.putPartitionState(topicPartition, OnlinePartition)
          else
            controllerContext.putPartitionState(topicPartition, OfflinePartition)
        case None =>
          controllerContext.putPartitionState(topicPartition, NewPartition)
      }
    }
  }

  def handleStateChanges(
                          partitions: Seq[TopicPartition],
                          targetState: PartitionState
                        ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    handleStateChanges(partitions, targetState, None)
  }

  def handleStateChanges(
                          partitions: Seq[TopicPartition],
                          targetState: PartitionState,
                          leaderElectionStrategy: Option[PartitionLeaderElectionStrategy]
                        ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]]

}

/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. NonExistentPartition: This state indicates that the partition was either never created or was created and then
 * deleted. Valid previous state, if one exists, is OfflinePartition
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 * replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 * Valid previous states are NewPartition/OfflinePartition
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 * moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
 */
class ZkPartitionStateMachine(config: KafkaConfig,
                              stateChangeLogger: StateChangeLogger,
                              controllerContext: ControllerContext,
                              zkClient: KafkaZkClient,
                              controllerBrokerRequestBatch: ControllerBrokerRequestBatch)
  extends PartitionStateMachine(controllerContext) {

  private val controllerId = config.brokerId
  this.logIdent = s"[PartitionStateMachine controllerId=$controllerId] "

  /**
   * 这个方法有两个操作步骤：
   * ① 将给定的分区状态变更为目标状态
   * ② 可能需要使用分区leader选举策略为分区选举新的Leader
   * ③ 将相关变更请求发送给其它Broker，通知它们做相应的操作
   * @param partitions                         待执行状态变量的目标分区列表
   * @param targetState                        目标状态
   * @param partitionLeaderElectionStrategyOpt 分区Leader选举策略
   * @return 一个Map集合，value包含Leader分区结果和ISR集合或异常（互斥）。
   */
  override def handleStateChanges(
                                   partitions: Seq[TopicPartition],
                                   targetState: PartitionState,
                                   partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy]
                                 ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    if (partitions.nonEmpty) {
      try {
        // #1 判断Controller请求缓存是否全部为空，只要有一个不为空，抛出异常
        // 这一步确保每轮的请求发送都是独立的，不会和上一次混淆
        controllerBrokerRequestBatch.newBatch()

        // #2 执行真正的状态变更逻辑
        val result = doHandleStateChanges(
          partitions,
          targetState,
          partitionLeaderElectionStrategyOpt
        )

        // #3 将请求缓存真正发送给Broker端
        controllerBrokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)

        // #4 返回状态变更处理结果
        result
      } catch {
        // #5 如果Controller发送变更，则记录日志并抛出异常，上层调用者会捕获异常并进行Controller卸任逻辑
        case e: ControllerMovedException =>
          error(s"Controller moved to another broker when moving some partitions to $targetState state", e)
          throw e
        case e: Throwable =>
          error(s"Error while moving some partitions to $targetState state", e)
          partitions.iterator.map(_ -> Left(e)).toMap
      }
    } else {
      Map.empty
    }
  }

  private def partitionState(partition: TopicPartition): PartitionState = {
    controllerContext.partitionState(partition)
  }

  /**
   * 分区状态机核心代码，确保每次状态转换都是合法的。
   * ① NonExistentPartition -> NewPartition：从ZK中加载已分配的副本元数据到controller缓存中。
   * ② NewPartition -> OnlinePartition：
   * 1.将第一个存活的副本当作分区的Leader副本，其它所有存活的副本当作ISR集合（即包含Leader副本）
   * 2.将分区分配结果（Leader副本、ISR副本集合）写回所对应的ZK节点中。
   * 3.向所有存活的副本所在的Broker发送「LeaderAndIsr」请求。
   * 4.向集群广播「UpdateMetadata」请求。
   * ③ OnlinePartition,OfflinePartition -> OnlinePartition
   * 1.使用副本Leader选举策略选出一个Leader，确认ISR集合。
   * 2.将分区分配结果（Leader副本、ISR副本集合）写回所对应的ZK节点中。
   * 3.向每个可接收的副本发送「LeaderAndIsr」请求。
   * 4.向集群广播「UpdateMetadata」请求。
   * ④ NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition
   * 1.将分区状态设置为「OfflinePartition」
   * ⑤ OfflinePartition -> NonExistentPartition
   * 1.将分区状态设置为「NonExistentPartition」
   *
   * @param partitions                         分区列表
   * @param targetState                        目标状态
   * @param partitionLeaderElectionStrategyOpt 副本Leader选举策略
   * @return
   * A map of failed and successful elections when targetState is OnlinePartitions. The keys are the
   * topic partitions and the corresponding values are either the exception that was thrown or new
   * leader & ISR.
   */
  private def doHandleStateChanges(
                                    partitions: Seq[TopicPartition],
                                    targetState: PartitionState,
                                    partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy]
                                  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    val traceEnabled = stateChangeLog.isTraceEnabled

    // #1 如果分区在缓存（partitionStates）中不存在相关状态，那么将它初始化为「NonExistentPartition」
    partitions.foreach(partition => controllerContext.putPartitionStateIfNotExists(partition, NonExistentPartition))

    // #2 遍历待转换的分区，根据当前状态和目标状态判断本次转换操作是否合法，如果不合法，进行日志记录，
    //    只进行合法的状态变更操作
    val (validPartitions, invalidPartitions) = controllerContext.checkValidPartitionStateChange(partitions, targetState)
    invalidPartitions.foreach(partition => logInvalidTransition(partition, targetState))

    // #3 状态变更
    targetState match {
      // #3-1 目标状态为NewPartition
      case NewPartition =>
        validPartitions.foreach { partition =>
          stateChangeLog.info(s"Changed partition $partition state from ${partitionState(partition)} to $targetState with " +
            s"assigned replicas ${controllerContext.partitionReplicaAssignment(partition).mkString(",")}")
          controllerContext.putPartitionState(partition, NewPartition)
        }
        Map.empty

      // #3-2 分区上线
      case OnlinePartition =>
        // ① 获取未初始化分区列表，即「NewPartition」状态下的所有分区
        val uninitializedPartitions = validPartitions.filter(partition => partitionState(partition) == NewPartition)

        // ② 获取具备Leader选举的分区列表（当前状态为 OfflinePartition 或 OnlinePartition 才能进行分区Leader选举）
        val partitionsToElectLeader = validPartitions
          .filter(partition => partitionState(partition) == OfflinePartition || partitionState(partition) == OnlinePartition)

        // ③ 初始化「NewPartition」状态分区，在Zookeeper中写入Leader和ISR数据
        if (uninitializedPartitions.nonEmpty) {
          // 初始化Zookeeper节点/brokers/topics/<topic>/partitions/<partition>数据
          val successfulInitializations = initializeLeaderAndIsrForPartitions(uninitializedPartitions)
          successfulInitializations.foreach { partition =>
            stateChangeLog.info(s"Changed partition $partition from ${partitionState(partition)} to $targetState with state " +
              s"${controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr}")
            // NewPartition->OnlinePartition
            controllerContext.putPartitionState(partition, OnlinePartition)
          }
        }

        // ④ 使用Leader副本选举策略给具备Leader选举资格的分区选举Leader副本
        if (partitionsToElectLeader.nonEmpty) {
          // 不断尝试为多个分区选举Leader，直到所有分区都成功选出Leader
          val electionResults = electLeaderForPartitions(
            partitionsToElectLeader,
            partitionLeaderElectionStrategyOpt.getOrElse(
              throw new IllegalArgumentException("Election strategy is a required field when the target state is OnlinePartition")
            )
          )

          // 遍历选举结果
          electionResults.foreach {
            case (partition, Right(leaderAndIsr)) =>
              stateChangeLog.info(
                s"Changed partition $partition from ${partitionState(partition)} to $targetState with state $leaderAndIsr"
              )
              // ⑤ 将成功选举Leader后的分区设置成OnlinePartition状态
              controllerContext.putPartitionState(partition, OnlinePartition)
            case (_, Left(_)) => // Ignore; no need to update partition state on election error
          }

          // ⑥ 返回Leader选举结果
          electionResults
        } else {
          Map.empty
        }
      case OfflinePartition | NonExistentPartition =>
        validPartitions.foreach { partition =>
          if (traceEnabled)
            stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState")
          controllerContext.putPartitionState(partition, targetState)
        }
        Map.empty
    }
  }

  /**
   * 更新Zookeeper数据，节点/brokers/topics/<topic>/partitions/<partition>
   * 数据包含Leader分区和ISR集合
   * 1.从缓存中获取分区副本元数据
   * 2.获取分区所有存活的副本集合
   * 3.按有无存活副本对分区进行分组=>（有存活副本的分区,无存活副本的分区）
   * 4.对无存活副本的分区日志记录
   * 5.为有存活副本的分区确定Leader和ISR集合
   * ① Leader：存活副本列表首个副本被认定为Leader
   * ② ISR：存活副本列表被认定为 ISR集合
   * 6.创建ZK相关节点（持久化元数据信息）
   *
   * @param partitions 待初始化的分区集合
   * @return The partitions that have been successfully initialized.
   */
  private def initializeLeaderAndIsrForPartitions(partitions: Seq[TopicPartition]): Seq[TopicPartition] = {
    val successfulInitializations = mutable.Buffer.empty[TopicPartition]
    // #1 获取每个分区的副本列表
    val replicasPerPartition = partitions.map(partition => partition -> controllerContext.partitionReplicaAssignment(partition))

    // #2 获取每个分区的所有存活副本
    val liveReplicasPerPartition = replicasPerPartition.map { case (partition, replicas) =>
      val liveReplicasForPartition = replicas.filter(replica => controllerContext.isReplicaOnline(replica, partition))
      partition -> liveReplicasForPartition
    }

    // #3 按照有无存活副本对分区进行分组
    val (partitionsWithoutLiveReplicas, partitionsWithLiveReplicas) =
      liveReplicasPerPartition.partition { case (_, liveReplicas) => liveReplicas.isEmpty }

    partitionsWithoutLiveReplicas.foreach { case (partition, replicas) =>
      val failMsg = s"Controller $controllerId epoch ${controllerContext.epoch} encountered error during state change of " +
        s"partition $partition from New to Online, assigned replicas are " +
        s"[${replicas.mkString(",")}], live brokers are [${controllerContext.liveBrokerIds}]. No assigned " +
        "replica is alive."
      logFailedStateChange(partition, NewPartition, OnlinePartition, new StateChangeFailedException(failMsg))
    }

    // #4 为有存活的副本集合确定Leader和ISR
    //    Leader：ISR集合中第一个满足条件的即可成为Leader。ISR：存活的副本集合即为ISR
    val leaderIsrAndControllerEpochs = partitionsWithLiveReplicas.map { case (partition, liveReplicas) =>
      val leaderAndIsr = LeaderAndIsr(liveReplicas.head, liveReplicas.toList)
      val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
      partition -> leaderIsrAndControllerEpoch
    }.toMap

    // #5 创建ZK相关节点
    val createResponses = try {
      zkClient.createTopicPartitionStatesRaw(leaderIsrAndControllerEpochs, controllerContext.epochZkVersion)
    } catch {
      case e: ControllerMovedException =>
        error("Controller moved to another broker when trying to create the topic partition state znode", e)
        throw e
      case e: Exception =>
        partitionsWithLiveReplicas.foreach { case (partition, _) => logFailedStateChange(partition, partitionState(partition), NewPartition, e) }
        Seq.empty
    }
    createResponses.foreach { createResponse =>
      val code = createResponse.resultCode
      val partition = createResponse.ctx.get.asInstanceOf[TopicPartition]
      val leaderIsrAndControllerEpoch = leaderIsrAndControllerEpochs(partition)
      if (code == Code.OK) {
        controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
        controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(leaderIsrAndControllerEpoch.leaderAndIsr.isr,
          partition, leaderIsrAndControllerEpoch, controllerContext.partitionFullReplicaAssignment(partition), isNew = true)
        successfulInitializations += partition
      } else {
        logFailedStateChange(partition, NewPartition, OnlinePartition, code)
      }
    }
    successfulInitializations
  }

  /**
   * Repeatedly attempt to elect leaders for multiple partitions until there are no more remaining partitions to retry.
   *
   * @param partitions                      The partitions that we're trying to elect leaders for.
   * @param partitionLeaderElectionStrategy The election strategy to use.
   * @return A map of failed and successful elections. The keys are the topic partitions and the corresponding values are
   *         either the exception that was thrown or new leader & ISR.
   */
  private def electLeaderForPartitions(partitions: Seq[TopicPartition],
                                        partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy
                                      ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    var remaining = partitions
    val finishedElections = mutable.Map.empty[TopicPartition, Either[Throwable, LeaderAndIsr]]

    while (remaining.nonEmpty) {
      val (finished, updatesToRetry) = doElectLeaderForPartitions(remaining, partitionLeaderElectionStrategy)
      remaining = updatesToRetry

      finished.foreach {
        case (partition, Left(e)) =>
          logFailedStateChange(partition, partitionState(partition), OnlinePartition, e)
        case (_, Right(_)) => // Ignore; success so no need to log failed state change
      }

      finishedElections ++= finished

      if (remaining.nonEmpty)
        logger.info(s"Retrying leader election with strategy $partitionLeaderElectionStrategy for partitions $remaining")
    }

    finishedElections.toMap
  }

  /**
   * 使用分区Leader选举策略为分区选出Leader副本，并将结果持久化至ZK相关节点
   *
   * @param partitions                      试图对这些分区进行Leader副本选举操作
   * @param partitionLeaderElectionStrategy Leader副本选举策略，详见 {@link PartitionLeaderElectionStrategy}
   * @return 返回一个Tuple2元组。
   *         _1：要么是成功选出Leader和确认ISR，要么出现不可重试的异常
   *         _2：需要重试的分区集合。由于ZK的「BADVERSION」冲突而导致将元数据持久化至ZK节点失败。
   *         如果分区Leader更新分区状态的同时controller也尝试更新分区状态，这种情况这会出现ZK版本冲突。重试就能解决（有点类似CAS思想）。
   */
  private def doElectLeaderForPartitions(partitions: Seq[TopicPartition],
                                         partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy
                                        ): (Map[TopicPartition, Either[Exception, LeaderAndIsr]], Seq[TopicPartition]) = {

    // PART 1：数据准备
    // #1-1 批量从ZK路径「/brokers/topics<topic name>/partitions/<partition id>/state」中获取分区状态数据
    // Data	{"controller_epoch":1,"leader":1,"version":1,"leader_epoch":4,"isr":[1]}
    val getDataResponses = try {
      zkClient.getTopicPartitionStatesRaw(partitions)
    } catch {
      case e: Exception =>
        return (partitions.iterator.map(_ -> Left(e)).toMap, Seq.empty)
    }
    // 构建两个容器，分别保存可选举Leader分区列表和选举失败分区列表
    val failedElections = mutable.Map.empty[TopicPartition, Either[Exception, LeaderAndIsr]]
    // 保存ZK的分区节点元数据（Leader副本、ISR集合）
    val validLeaderAndIsrs = mutable.Buffer.empty[(TopicPartition, LeaderAndIsr)]

    // #1-2 解析数据，填充上面的两个集合
    getDataResponses.foreach { getDataResponse =>
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      // 分区当前在缓存中的状态
      val currState = partitionState(partition)
      if (getDataResponse.resultCode == Code.OK) {
        TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat) match {
          case Some(leaderIsrAndControllerEpoch) =>
            // #1-3 如果Zookeeper节点数据的Controller Epoch值大于缓存中Controller Epoch值，
            //      表示该分区已经被一个更新的Controller选举过Leader，此时必须终止本次Leader选举，并将该分区放置到选举失败分区列表中
            if (leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch) {
              val failMsg = s"Aborted leader election for partition $partition since the LeaderAndIsr path was " +
                s"already written by another controller. This probably means that the current controller $controllerId went through " +
                s"a soft failure and another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}."
              // ZK版本冲突，说明有其它的线程抢先修改了节点的数据，需要进行重试
              failedElections.put(partition, Left(new StateChangeFailedException(failMsg)))
            } else {
              // #1-4 将ZK数据保存到集合中
              validLeaderAndIsrs += partition -> leaderIsrAndControllerEpoch.leaderAndIsr
            }
          case None =>
            val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
            failedElections.put(partition, Left(exception))
        }
      } else if (getDataResponse.resultCode == Code.NONODE) {
        val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
        failedElections.put(partition, Left(exception))
      } else {
        failedElections.put(partition, Left(getDataResponse.resultException.get))
      }
    }

    if (validLeaderAndIsrs.isEmpty) {
      return (failedElections.toMap, Seq.empty)
    }

    // PART 2：使用Leader选举策略进行Leader副本选举
    val (partitionsWithoutLeaders, partitionsWithLeaders) = partitionLeaderElectionStrategy match {
      // 匹配不同的leader选举策略，这也是根据触发条件而决定的，
      // 比如原有的Leader副本离线了，就会使用OfflinePartitionLeaderElectionStrategy策略
      case OfflinePartitionLeaderElectionStrategy(allowUnclean) =>
        // 离线分区选举策略有一点重要的点是：是否支持 unclean leader 选举
        val partitionsWithUncleanLeaderElectionState = collectUncleanLeaderElectionState(
          validLeaderAndIsrs,
          allowUnclean
        )
        leaderForOffline(controllerContext, partitionsWithUncleanLeaderElectionState).partition(_.leaderAndIsr.isEmpty)
      case ReassignPartitionLeaderElectionStrategy =>
        // 为副本重分区的分区选举Leader
        leaderForReassign(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
      case PreferredReplicaPartitionLeaderElectionStrategy =>
        // 执行preferred-replica选举
        leaderForPreferredReplica(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
      case ControlledShutdownPartitionLeaderElectionStrategy =>
        // 因Broker正常关闭而受影响的分区选举Leader
        leaderForControlledShutdown(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
    }
    partitionsWithoutLeaders.foreach { electionResult =>
      val partition = electionResult.topicPartition
      val failMsg = s"Failed to elect leader for partition $partition under strategy $partitionLeaderElectionStrategy"
      failedElections.put(partition, Left(new StateChangeFailedException(failMsg)))
    }

    // PART 3：将所有选举失败的分区全部加入到Leader选举失败分区列表
    // 更新Zookeeper数据+更新Controller缓存
    val recipientsPerPartition = partitionsWithLeaders.map(result => result.topicPartition -> result.liveReplicas).toMap
    val adjustedLeaderAndIsrs = partitionsWithLeaders.map(result => result.topicPartition -> result.leaderAndIsr.get).toMap

    // 更新ZK节点数据
    val UpdateLeaderAndIsrResult(finishedUpdates, updatesToRetry) = zkClient.updateLeaderAndIsr(
      adjustedLeaderAndIsrs, controllerContext.epoch, controllerContext.epochZkVersion)
    finishedUpdates.forKeyValue { (partition, result) =>
      result.foreach { leaderAndIsr =>
        val replicaAssignment = controllerContext.partitionFullReplicaAssignment(partition)
        val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
        // 更新缓存
        controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
        // 构建LeaderAndIsrRequest
        controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(
          recipientsPerPartition(partition),
          partition,
          leaderIsrAndControllerEpoch,
          replicaAssignment,
          isNew = false)
      }
    }

    (finishedUpdates ++ failedElections, updatesToRetry)
  }

  /* For the provided set of topic partition and partition sync state it attempts to determine if unclean
   * leader election should be performed. Unclean election should be performed if there are no live
   * replica which are in sync and unclean leader election is allowed (allowUnclean parameter is true or
   * the topic has been configured to allow unclean election).
   *
   * @param leaderIsrAndControllerEpochs set of partition to determine if unclean leader election should be
   *                                     allowed
   * @param allowUnclean whether to allow unclean election without having to read the topic configuration
   * @return a sequence of three element tuple:
   *         1. topic partition
   *         2. leader, isr and controller epoc. Some means election should be performed
   *         3. allow unclean
   */
  private def collectUncleanLeaderElectionState(
                                                 leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)],
                                                 allowUnclean: Boolean
                                               ): Seq[(TopicPartition, Option[LeaderAndIsr], Boolean)] = {
    val (partitionsWithNoLiveInSyncReplicas, partitionsWithLiveInSyncReplicas) = leaderAndIsrs.partition {
      case (partition, leaderAndIsr) =>
        val liveInSyncReplicas = leaderAndIsr.isr.filter(controllerContext.isReplicaOnline(_, partition))
        liveInSyncReplicas.isEmpty
    }

    val electionForPartitionWithoutLiveReplicas = if (allowUnclean) {
      partitionsWithNoLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
        (partition, Option(leaderAndIsr), true)
      }
    } else {
      val (logConfigs, failed) = zkClient.getLogConfigs(
        partitionsWithNoLiveInSyncReplicas.iterator.map { case (partition, _) => partition.topic }.toSet,
        config.originals()
      )

      partitionsWithNoLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
        if (failed.contains(partition.topic)) {
          logFailedStateChange(partition, partitionState(partition), OnlinePartition, failed(partition.topic))
          (partition, None, false)
        } else {
          (
            partition,
            Option(leaderAndIsr),
            logConfigs(partition.topic).uncleanLeaderElectionEnable.booleanValue()
          )
        }
      }
    }

    electionForPartitionWithoutLiveReplicas ++
      partitionsWithLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
        (partition, Option(leaderAndIsr), false)
      }
  }

  private def logInvalidTransition(partition: TopicPartition, targetState: PartitionState): Unit = {
    val currState = partitionState(partition)
    val e = new IllegalStateException(s"Partition $partition should be in one of " +
      s"${targetState.validPreviousStates.mkString(",")} states before moving to $targetState state. Instead it is in " +
      s"$currState state")
    logFailedStateChange(partition, currState, targetState, e)
  }

  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, code: Code): Unit = {
    logFailedStateChange(partition, currState, targetState, KeeperException.create(code))
  }

  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, t: Throwable): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .error(s"Controller $controllerId epoch ${controllerContext.epoch} failed to change state for partition $partition " +
        s"from $currState to $targetState", t)
  }
}

/**
 * 针对4种触发的Leader选举的场景，这个对象分别定义了4个方法，负责为每种场景选举Leader副本
 */
object PartitionLeaderElectionAlgorithms {

  /**
   * 场景一：由于Leader副本下线而引发分区Leader选举
   * 选举策略：AR集合第一个满足存在①副本在线②在ISR集合中，它就是Leader副本
   *
   * @param assignment                   分区副本列表，即AR（Assigned Replicas，感觉也可以称为All Replicas）
   *                                     使用Seq表示AR是有序的，但顺序不一定和ISR相同，因为ISR可能会频繁进出
   * @param isr                          同步副本集合。注意：Leader副本也在ISR集合中。
   * @param liveReplicas                 该分区下所有存活的副本。
   * @param uncleanLeaderElectionEnabled 是否允许Unclean Leader副本参与Leader选举。这可能存在数据丢失的风险
   * @param controllerContext            Controller上下文，里面保留集群所有元数据
   * @return
   */
  def offlinePartitionLeaderElection(assignment: Seq[Int],
                                     isr: Seq[Int],
                                     liveReplicas: Set[Int],
                                     uncleanLeaderElectionEnabled: Boolean,
                                     controllerContext: ControllerContext): Option[Int] = {

    // #1 按顺序搜索AR列表，如果同时满足①副本所在的Broker仍在运行;②副本在ISR列表中 这两个条件，则表明找到Leader副本。
    //    否则判断是否开启Unclean Leader选举，如果开启，则从Unclean Leader副本中选出一个作为Leader副本以保证分区可用性
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id)).orElse {
      // #2 ISR集合没有可用的副本对象，查看是否允许Unclean Leader选举（即unclean.leader.election.enable=true）
      if (uncleanLeaderElectionEnabled) {
        // #3 选举当前副本列表中第一个存活副本作为Leader
        val leaderOpt = assignment.find(liveReplicas.contains)
        if (leaderOpt.isDefined)
          controllerContext.stats.uncleanLeaderElectionRate.mark()
        leaderOpt
      } else {
        // 如果不允许Unclean Leader选举，则返回None表示无法选举Leader
        None
      }
    }
  }

  /**
   * 场景二：由于执行分区重分配操作而引发的分区Leader选举
   *
   * @param reassignment
   * @param isr
   * @param liveReplicas
   * @return
   */
  def reassignPartitionLeaderElection(reassignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    reassignment.find(id => liveReplicas.contains(id) && isr.contains(id))
  }

  /**
   * 场景三：由于执行Preferred副本Leader选举而引发的分区Leader选举
   * 出现这个选举策略是因为解决Leader分配不均的问题。
   * 当所有集群正常并且同时重启时，副本Leader会均匀分布，但随着时间推移，避免不了某些Broker宕机，
   * 这样在其它Broker的follower成为Leader，即便后续后续原有的Leader重启成功，
   * 即便奋起直追，加入ISR列表，它的身份也只是Follower。久而久之，就会导致整个集群的流量不均匀，加大其它Broker宕机风险。
   * Kakfa提供kakfa-preferred-replica-election.sh脚本去均衡分区的Leader副本，实现思路是：
   * 1.引入preferred-replica概念，它是指ISR列表中第一个replicaid就是preferred-replica。
   * 最初的Leader肯定是排在ISR列表的首位，但是Broker宕机后变成follower，但是ISR中的preferred-replica不会改变，
   * 执行kakfa-preferred-replica-election.sh脚本就是让preferred-replica重新成为分区Leader副本。
   * 这是手动触发的，也可以配置「auto.leader.rebalance.enable=true」让Kafka满足一定条件时自动触发（Kafka监控集群不均衡度达到某个阈值时自动触发preferred-replica选举操作）。
   *
   * @param assignment
   * @param isr
   * @param liveReplicas
   * @return
   */
  def preferredReplicaPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    // 判断第一个副本是否存活
    assignment.headOption.filter(id => liveReplicas.contains(id) && isr.contains(id))
  }

  /**
   * 场景四：因为正常关闭Broker而引发分区Leader选举
   *
   * @param assignment
   * @param isr
   * @param liveReplicas
   * @param shuttingDownBrokers
   * @return
   */
  def controlledShutdownPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int], shuttingDownBrokers: Set[Int]): Option[Int] = {
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id) && !shuttingDownBrokers.contains(id))
  }
}

/**
 * 分区Leader选举策略接口
 */
sealed trait PartitionLeaderElectionStrategy

/**
 * 离线分区Leader选举策略
 * 场景一：由于Leader副本下线而引发分区Leader选举
 *
 * @param allowUnclean
 */
final case class OfflinePartitionLeaderElectionStrategy(allowUnclean: Boolean) extends PartitionLeaderElectionStrategy

/**
 * 分区副本重分配Leader选举策略
 * 场景二：由于执行分区重分配操作而引发的分区Leader选举
 */
final case object ReassignPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy

/**
 * 分区Preferred(优先)副本Leader选举策略
 * 场景三：由于执行Preferred副本Leader选举而引发的分区Leader选举
 */
final case object PreferredReplicaPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy

/**
 * Controller关闭时Leader选举策略
 * 场景四：因为正常关闭Broker而引发分区Leader选举
 */
final case object ControlledShutdownPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy

sealed trait PartitionState {
  def state: Byte

  def validPreviousStates: Set[PartitionState]
}

/**
 * 分区被创建后被设置成此状态，表明它是一个全新分区对象。
 * 处于此状态的分区不能被选举成为Leader。
 */
case object NewPartition extends PartitionState {
  val state: Byte = 0
  val validPreviousStates: Set[PartitionState] = Set(NonExistentPartition)
}

/**
 * 在线。分区正式提供服务
 */
case object OnlinePartition extends PartitionState {
  val state: Byte = 1
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

/**
 * 离线分区
 */
case object OfflinePartition extends PartitionState {
  val state: Byte = 2
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

/**
 * 分区被删除
 */
case object NonExistentPartition extends PartitionState {
  val state: Byte = 3
  val validPreviousStates: Set[PartitionState] = Set(OfflinePartition)
}
