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
package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

/**
 * 消息累加器
 * 类似一个队列，它会不断把消息累加到 {@link MemoryRecords} 对象中，
 * 这些消息将会被发送到相应的 Broker
 * <p>
 * ① 消息累加器使用了一个固定的内存空间，当空间不足时，累加动作会被阻塞
 * ② 可以显示停止累加动作
 */
public final class RecordAccumulator {

    private final Logger log;
    private volatile boolean closed;
    private final AtomicInteger flushesInProgress;
    private final AtomicInteger appendsInProgress;

    // 批次大小，默认值：16384
    private final int batchSize;

    // 压缩类型，默认值：NONE，不压缩
    private final CompressionType compression;

    // 等待时间，默认值：0 表示即使批次不满也会发送到对端
    private final int lingerMs;

    // 重试退避时间，默认值：100
    private final long retryBackoffMs;

    // 默认值：120000
    private final int deliveryTimeoutMs;

    // ByteBuffer 对象池，用于分配一定大小的 ByteBuffer 对象
    private final BufferPool free;

    // 时间工具
    private final Time time;

    private final ApiVersions apiVersions;
    // CopyOnWriteMap 实例对象，用来保存消息对象
    // batches对象包含多个分区数据，一个分区含有多个批次，一个批次含有多条消息
    private final ConcurrentMap<TopicPartition, Deque<ProducerBatch>> batches;

    // 持有还未收到 ACK 批次引用，包含还未发送的批次
    private final IncompleteBatches incomplete;

    // 接下来的变量只会被「sender」线程读取，所以我们不需要同步
    private final Set<TopicPartition> muted;

    // 保存抽取的分区序号
    private int drainIndex;

    // 事务处理器
    private final TransactionManager transactionManager;

    // 一个批次过期的最早时间
    private long nextBatchExpiryTimeMs = Long.MAX_VALUE;

    /**
     * Create a new record accumulator
     *
     * @param logContext The log context used for logging
     * @param batchSize The size to use when allocating {@link MemoryRecords} instances
     * @param compression The compression codec for the records
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *        exhausting all retries in a short period of time.
     * @param metrics The metrics
     * @param time The time instance to use
     * @param apiVersions Request API versions for current connected brokers
     * @param transactionManager The shared transaction state object which tracks producer IDs, epochs, and sequence
     *                           numbers per partition.
     */
    public RecordAccumulator(LogContext logContext,
                             int batchSize,
                             CompressionType compression,
                             int lingerMs,
                             long retryBackoffMs,
                             int deliveryTimeoutMs,
                             Metrics metrics,
                             String metricGrpName,
                             Time time,
                             ApiVersions apiVersions,
                             TransactionManager transactionManager,
                             BufferPool bufferPool) {
        this.log = logContext.logger(RecordAccumulator.class);
        this.drainIndex = 0;
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoffMs = retryBackoffMs;
        this.deliveryTimeoutMs = deliveryTimeoutMs;
        this.batches = new CopyOnWriteMap<>();
        this.free = bufferPool;
        this.incomplete = new IncompleteBatches();
        this.muted = new HashSet<>();
        this.time = time;
        this.apiVersions = apiVersions;
        this.transactionManager = transactionManager;
        registerMetrics(metrics, metricGrpName);
    }

    private void registerMetrics(Metrics metrics, String metricGrpName) {
        MetricName metricName = metrics.metricName("waiting-threads", metricGrpName, "The number of user threads blocked waiting for buffer memory to enqueue their records");
        Measurable waitingThreads = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.queued();
            }
        };
        metrics.addMetric(metricName, waitingThreads);

        metricName = metrics.metricName("buffer-total-bytes", metricGrpName, "The maximum amount of buffer memory the client can use (whether or not it is currently used).");
        Measurable totalBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.totalMemory();
            }
        };
        metrics.addMetric(metricName, totalBytes);

        metricName = metrics.metricName("buffer-available-bytes", metricGrpName, "The total amount of buffer memory that is not being used (either unallocated or in the free list).");
        Measurable availableBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.availableMemory();
            }
        };
        metrics.addMetric(metricName, availableBytes);
    }

    /**
     * 向「消息累加器」添加一条记录，并返回添加结果
     * ① 异步过程，返回的结果包含将来的元信息（future metadata）
     * ② 返回的结果包含一个标志位：表示被添加的批次是否已经满了或一个新的批次被创建
     * ③ 「添加操作」需要考虑缓冲区使用情况，同时也考虑多线程环境下的共享变量竞争问题。
     *    如果在多线程环境下保证添加操作在粒度更小的范围内执行
     * ④ 源码中两次对「dq」加锁，主要目的是减少锁的持有时间。
     *
     * @param tp              将记录（record）发往的TOPIC所属的分区
     * @param timestamp       记录被创建时的时间戳
     * @param key             记录的key值
     * @param value           记录的value值
     * @param headers         记录的头部
     * @param callback        用户指定的回调函数，当请求完成后会被回调
     * @param maxTimeToBlock  最大阻塞时长。如果累加器内部的 buffer 缓存已满，则添加操作会被阻塞，这个就是设置阻塞时长。单位：毫秒
     * @param abortOnNewBatch boolean变量，true 表示如果一个新批次被创建则返回并在尝试两次添加之前运行分区的「onNewBatch」方法
     * @param nowMs           当前时间戳
     */
    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Header[] headers,
                                     Callback callback,
                                     long maxTimeToBlock,
                                     boolean abortOnNewBatch,
                                     long nowMs) throws InterruptedException {
        // #1 记录正在处理消息数量
        appendsInProgress.incrementAndGet();
        ByteBuffer buffer = null;
        if (headers == null) headers = Record.EMPTY_HEADERS;
        try {
            // #2 根据主题名称+分区获取对应的双端队列，没有则创建，所以不会返回null
            Deque<ProducerBatch> dq = getOrCreateDeque(tp);

            // #3 加锁操作，因为「dq」是共享变量
            synchronized (dq) {
                if (closed)
                    throw new KafkaException("Producer closed while send in progress");
                // #3-1 尝试向一个「ProducerBatch」对象中添加这条消息记录
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq, nowMs);
                if (appendResult != null)
                    // #3-2 返回的结果不为null，说明添加成功，可以直接返回
                    return appendResult;
            }

            // #4 需要创建新的「ProducerBatch」且「abortOnNewBatch=true」，则直接返回
            //    主要目的是为了调用「Partitioner.onNewBatch」方法
            if (abortOnNewBatch) {
                // Return a result that will cause another call to append.
                return new RecordAppendResult(null, false, false, true);
            }

            byte maxUsableMagic = apiVersions.maxUsableProduceMagic();

            // 预估消息大小（单位：字节）
            int size = Math.max(this.batchSize, AbstractRecords.estimateSizeInBytesUpperBound(maxUsableMagic, compression, key, value, headers));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {} with remaining timeout {}ms", size, tp.topic(), tp.partition(), maxTimeToBlock);

            // #5 向ByteBufPool申请（阻塞操作），所以不应该锁住
            buffer = free.allocate(size, maxTimeToBlock);

            // #6 第二次对「dq」加锁
            nowMs = time.milliseconds();
            synchronized (dq) {
                // Need to check if producer is closed again after grabbing the dequeue lock.
                if (closed)
                    throw new KafkaException("Producer closed while send in progress");

                // #6-1 尝试将消息添加到批次中
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq, nowMs);
                if (appendResult != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
                    return appendResult;
                }

                // #6-2 使用「MemoryRecordsBuilder」管理ByteBuffer的增加、扩容等操作
                MemoryRecordsBuilder recordsBuilder = recordsBuilder(buffer, maxUsableMagic);

                // #6-3 创建一个新的「ProducerBatch」对象，每个对象包含对应的「MemoryRecordsBuilder」，这个对象用来管理「ByteBuffer」
                ProducerBatch batch = new ProducerBatch(tp, recordsBuilder, nowMs);

                // #6-4 这次总能添加成功了吧
                FutureRecordMetadata future = Objects.requireNonNull(batch.tryAppend(timestamp, key, value, headers,
                        callback, nowMs));
                // #6-5 加入队列
                dq.addLast(batch);
                // #6-6 以「批次」为单位添加到「未完成Set集合」中
                incomplete.add(batch);

                // #6-7 将ByteBuffer的引用置为null
                buffer = null;
                // #7 return
                //    batchIsFull 含义是：只要有一个批次满了就算满了。这样会唤醒Sender线程发送数据，尽量减少数据延迟
                return new RecordAppendResult(future, dq.size() > 1 || batch.isFull(), true, false);
            }
        } finally {
            // 释放ByteBuffer对象
            if (buffer != null)
                free.deallocate(buffer);
            appendsInProgress.decrementAndGet();
        }
    }

    private MemoryRecordsBuilder recordsBuilder(ByteBuffer buffer, byte maxUsableMagic) {
        if (transactionManager != null && maxUsableMagic < RecordBatch.MAGIC_VALUE_V2) {
            throw new UnsupportedVersionException("Attempting to use idempotence with a broker which does not " +
                "support the required message format (v2). The broker must be version 0.11 or later.");
        }
        // 批次基准偏移量为0
        return MemoryRecords.builder(buffer, maxUsableMagic, compression, TimestampType.CREATE_TIME, 0L);
    }

    /**
     * 尝试将消息添加到一个 {@link ProducerBatch} 对象中
     * ① 如果 {@link ProducerBatch} 对象有空间，则添加成功
     * ② 如果已经满了，将返回 {@code null} 并且一个新的 {@link ProducerBatch} 对象被创建。
     * ③ 我们会关闭某个批次，这个批次不会被允许添加 record。以此来释放压缩缓冲区之类的资源。
     *    即避免 {@link ProducerBatch} 对象过大，长时间占用过多内存空间。
     * ④ 以下情况，一个批次将会被完全关闭：
     *    <1> 在发送前批次过期了
     *    <2> 生产者被关闭
     *
     * @param timestamp 记录创建时的时间
     * @param key       记录的key
     * @param value     记录的value
     * @param headers   记录的头部信息
     * @param callback  用户指定的回调函数，当请求完成后会被回调
     * @param deque     根据记录所发往的topic消息从 {@link #batches} 中获取双端队列
     * @param nowMs     此刻时间戳
     * @return 该消息添加结果
     */
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers,
                                         Callback callback, Deque<ProducerBatch> deque, long nowMs) {
        // #1 PEEK队尾数据
        ProducerBatch last = deque.peekLast();

        // #2 对象存在，则尝试添加记录。因为可能此对象已满，不允许添加了，所以才说「尝试」
        if (last != null) {
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, headers, callback, nowMs);
            if (future == null)
                // 当前「producerBatch」没有空间，尝试添加失败
                last.closeForRecordAppends();
            else
                // 尝试添加成功
                return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false, false);
        }
        return null;
    }

    private boolean isMuted(TopicPartition tp) {
        return muted.contains(tp);
    }

    public void resetNextBatchExpiryTime() {
        nextBatchExpiryTimeMs = Long.MAX_VALUE;
    }

    public void maybeUpdateNextBatchExpiryTime(ProducerBatch batch) {
        if (batch.createdMs + deliveryTimeoutMs  > 0) {
            // the non-negative check is to guard us against potential overflow due to setting
            // a large value for deliveryTimeoutMs
            nextBatchExpiryTimeMs = Math.min(nextBatchExpiryTimeMs, batch.createdMs + deliveryTimeoutMs);
        } else {
            log.warn("Skipping next batch expiry time update due to addition overflow: "
                + "batch.createMs={}, deliveryTimeoutMs={}", batch.createdMs, deliveryTimeoutMs);
        }
    }

    /**
     * 获得一份在消息累积器中放置太久、需要过期的批次列表
     * @param now  当前时间戳
     * @return
     */
    public List<ProducerBatch> expiredBatches(long now) {
        // #1 记录过期批次
        List<ProducerBatch> expiredBatches = new ArrayList<>();
        for (Map.Entry<TopicPartition, Deque<ProducerBatch>> entry : this.batches.entrySet()) {
            // expire the batches in the order of sending
            Deque<ProducerBatch> deque = entry.getValue();
            synchronized (deque) {
                while (!deque.isEmpty()) {
                    ProducerBatch batch = deque.getFirst();
                    if (batch.hasReachedDeliveryTimeout(deliveryTimeoutMs, now)) {
                        // 批次过期，弹出
                        deque.poll();
                        // 中断添加操作
                        batch.abortRecordAppends();
                        // 添加到过期集合中
                        expiredBatches.add(batch);
                    } else {
                        maybeUpdateNextBatchExpiryTime(batch);
                        break;
                    }
                }
            }
        }
        return expiredBatches;
    }

    public long getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    /**
     * Re-enqueue the given record batch in the accumulator. In Sender.completeBatch method, we check
     * whether the batch has reached deliveryTimeoutMs or not. Hence we do not do the delivery timeout check here.
     */
    public void reenqueue(ProducerBatch batch, long now) {
        batch.reenqueued(now);
        Deque<ProducerBatch> deque = getOrCreateDeque(batch.topicPartition);
        synchronized (deque) {
            if (transactionManager != null)
                // 按Sequence Number 插入到队列合适的位置中
                insertInSequenceOrder(deque, batch);
            else
                deque.addFirst(batch);
        }
    }

    /**
     * 将一个大的批次拆分成若干小的批次后再入队列
     * @param bigBatch  待拆分批次
     * @return          拆分成多少个小批次
     */
    public int splitAndReenqueue(ProducerBatch bigBatch) {
        // Reset the estimated compression ratio to the initial value or the big batch compression ratio, whichever
        // is bigger. There are several different ways to do the reset. We chose the most conservative one to ensure
        // the split doesn't happen too often.
        CompressionRatioEstimator.setEstimation(bigBatch.topicPartition.topic(), compression,
                                                Math.max(1.0f, (float) bigBatch.compressionRatio()));
        Deque<ProducerBatch> dq = bigBatch.split(this.batchSize);
        int numSplitBatches = dq.size();
        Deque<ProducerBatch> partitionDequeue = getOrCreateDeque(bigBatch.topicPartition);
        while (!dq.isEmpty()) {
            ProducerBatch batch = dq.pollLast();
            incomplete.add(batch);
            // We treat the newly split batches as if they are not even tried.
            synchronized (partitionDequeue) {
                if (transactionManager != null) {
                    // We should track the newly created batches since they already have assigned sequences.
                    transactionManager.addInFlightBatch(batch);
                    insertInSequenceOrder(partitionDequeue, batch);
                } else {
                    partitionDequeue.addFirst(batch);
                }
            }
        }
        return numSplitBatches;
    }

    /**
     * 我们做了很多额外的工作以保证队列中的批次是按Sequence Number排序的，重试的数据放入进来也保证在合适的位置。
     * 如果第一个处于「in-flight」的请求添加失败，那么后续所有处于「in-flight」的请求也将失败，因为当前序列号对Broker端是无效的。
     *
     * 此外，一旦出现批次重试情况，我们将该分区处于「in-flight」的请求降至1。因此，当后续批次按顺序返回时，它们将不得不放在队列中更靠后的位置。
     *
     * 请注意，这假设队列中具有指定序列的所有批次也具有当前的PID。
     * 如果PID已更改，我们将不会尝试重新排序消息，而是会抛出IllegalStateException。
     *
     * @param deque
     * @param batch
     */
    private void insertInSequenceOrder(Deque<ProducerBatch> deque, ProducerBatch batch) {
        // 幂等重试的批次需要有PID和序列号，如果没有，抛出异常
        if (batch.baseSequence() == RecordBatch.NO_SEQUENCE)
            throw new IllegalStateException("Trying to re-enqueue a batch which doesn't have a sequence even " + "though idempotency is enabled.");

        // 从本地缓存中获取第一个ProducerBatch对象，这个本地缓存每个批次最近发送的数据，
        // 用来做幂等判断和事务管理
        if (transactionManager.nextBatchBySequence(batch.topicPartition) == null)
            throw new IllegalStateException("We are re-enqueueing a batch which is not tracked as part of the in flight " +
                "requests. batch.topicPartition: " + batch.topicPartition + "; batch.baseSequence: " + batch.baseSequence());

        // 获取缓存中最开始的那条数据
        ProducerBatch firstBatchInQueue = deque.peekFirst();
        if (firstBatchInQueue != null && firstBatchInQueue.hasSequence() && firstBatchInQueue.baseSequence() < batch.baseSequence()) {
            // 传入的批次在不违反Sequence Number序列的情况下插入到队列中。我们需要在队列中找到合适的位置放入重试批次。
            List<ProducerBatch> orderedBatches = new ArrayList<>();
            while (deque.peekFirst() != null && deque.peekFirst().hasSequence() && deque.peekFirst().baseSequence() < batch.baseSequence()) {
                orderedBatches.add(deque.pollFirst());
            }

            log.debug("Reordered incoming batch with sequence {} for partition {}. It was placed in the queue at " +
                "position {}", batch.baseSequence(), batch.topicPartition, orderedBatches.size());
            // Either we have reached a point where there are batches without a sequence (ie. never been drained
            // and are hence in order by default), or the batch at the front of the queue has a sequence greater
            // than the incoming batch. This is the right place to add the incoming batch.
            deque.addFirst(batch);

            // Now we have to re insert the previously queued batches in the right order.
            for (int i = orderedBatches.size() - 1; i >= 0; --i) {
                deque.addFirst(orderedBatches.get(i));
            }

            // At this point, the incoming batch has been queued in the correct place according to its sequence.
        } else {
            deque.addFirst(batch);
        }
    }

    /**
     * 遍历消息缓冲区中的所有批次，判断该批次所对应的节点是否已经准备就绪。将会分为三种情况：
     * ① 如果批次对应的分区没有Leader元数据，则将topic名称添加到「unknownLeaderTopics」
     * ② 如果批次对应的分区有Leader元数据，根据重试退避时间、超时时间、批次是否已满等等条件判断节点是否已经准备好接收数据
     *
     * @param cluster 集群元数据
     * @param nowMs   当前时间戳
     * @return        包含已准备好接收数据节点（readyNodes）和无Leader的Topic列表以及下次检查节点的延时时间（nextReadyCheckDelayMs）
     */
    public ReadyCheckResult ready(Cluster cluster, long nowMs) {
        // #1 记录「准备就绪」节点
        Set<Node> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        // #2 记录「未知领导者」主题，待后续会重新获取主题对应的元数据
        Set<String> unknownLeaderTopics = new HashSet<>();

        // #3 free是内存池，free.queued()>0表示有其他线程正在阻塞于内存申请，
        //    换句话说就是内存申请此刻处于繁忙状态
        boolean exhausted = this.free.queued() > 0;

        // #4 遍历消息缓冲区中，「batches」key为分区详情，value为待发送给分区的由多个消息组成的批次
        //    这一步我们需要逐个判断每个分区的队头的ProducerBatch情况
        for (Map.Entry<TopicPartition, Deque<ProducerBatch>> entry : this.batches.entrySet()) {
            // #4-1 获取分区对应的消息队列
            Deque<ProducerBatch> deque = entry.getValue();
            synchronized (deque) {
                // #4-3 获取队列头部的批次用来判断节点是否可用
                ProducerBatch batch = deque.peekFirst();
                if (batch != null) {
                    // #4-4 获取分区详情
                    TopicPartition part = entry.getKey();

                    // #4-5 根据分区从集群元数据中获取分区所对应的「Leader」节点详情（每个分区都有Leader节点）
                    //      其他为follower节点，是不对外提供服务的（可能以后会提供读服务）
                    Node leader = cluster.leaderFor(part);
                    if (leader == null) {
                        // #4-6 topic没有Leader信息，则将topic添加到「unknownLeaderTopics」，后续会重新同步该topic的元数据
                        unknownLeaderTopics.add(part.topic());
                    } else if (!readyNodes.contains(leader) && !isMuted(part)) {
                        // #4-7 如果「readyNodes」没有包含且分区没有被沉默（muted），才能进行下一步
                        //      这里「沉默」是用来保证单个分区内消息顺序的
                        // #4-8 计算批次自创建以来已等待时间（currentTme-lastAttemptMs）
                        long waitedTimeMs = batch.waitedTimeMs(nowMs);

                        // #4-9 批次是否处于重试状态且是否处于「重试退避时间」内
                        //      通过重试退避时间限制生产者无限制重发消息从而造成资源浪费和加重网络拥塞。
                        //      退避时间机制在Kafka随处可见
                        boolean backingOff = batch.attempts() > 0 && waitedTimeMs < retryBackoffMs;
                        // #4-10 若当前批次处于重试退避时间范围内，则其还需要等待时间为「retryBackoffMs」
                        //       否则等待时间为lingerMs（默认时间为0）
                        long timeToWaitMs = backingOff ? retryBackoffMs : lingerMs;
                        // #4-11 队列「deque」是否存在消息已填满的批次
                        boolean full = deque.size() > 1 || batch.isFull();

                        // #4-12 判断批次等待时间是否已失效，如果已失效，则表示批次可以被发送
                        boolean expired = waitedTimeMs >= timeToWaitMs;

                        // #4-13 综合确定一个批次是否满足条件，可以发送
                        // ① 队列有已被写满的批次
                        // ② 超过设定的等待时长（expired）
                        // ③ BufferPool缓冲区有等待内存分配的线程，急需其它线程释放内存，这就意味着需要把数据发往broker才能释放内存
                        // ④ close() 方法被调用，生产者被关闭
                        // ⑤ flush() 方法被调用
                        boolean sendable = full || expired || exhausted || closed || flushInProgress();
                        if (sendable && !backingOff) {
                            readyNodes.add(leader);
                        } else {
                            // #4-15 计算下轮检查时间（取各种延迟情况的最小值）
                            //       这个时间是保守估计得到的，因为当前不可发送分区对应的leader在后面可以找到一个可发送的批次。
                            //       但是，这也足够好了因为我们只需要在剩余的时间内唤醒线程然后再让线程被休眠
                            long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                            // Note that this results in a conservative estimate since an un-sendable partition may have
                            // a leader that will later be found to have sendable data. However, this is good enough
                            // since we'll just wake up and then sleep again for the remaining time.
                            nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
                        }
                    }
                }
            }
        }
        // #5 返回检查结果
        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTopics);
    }

    /**
     * Check whether there are any batches which haven't been drained
     */
    public boolean hasUndrained() {
        for (Map.Entry<TopicPartition, Deque<ProducerBatch>> entry : this.batches.entrySet()) {
            Deque<ProducerBatch> deque = entry.getValue();
            synchronized (deque) {
                if (!deque.isEmpty())
                    return true;
            }
        }
        return false;
    }

    /**
     * 判断是否应该停止对该分区进行消息抽取
     * 1.如果不存在事务管理器，则不应该停止，返回false。
     * 2.存在事务管理器，根据「in-flight」和序列号判断是否可以从 {@link RecordAccumulator} 中抽取消息批次。
     *
     * @param first
     * @param tp
     * @return
     */
    private boolean shouldStopDrainBatchesForPartition(ProducerBatch first, TopicPartition tp) {
        ProducerIdAndEpoch producerIdAndEpoch;
        // 存在本地事务管理器
        if (transactionManager != null) {
            // 分区是否处于事务中，如果是事务生产者且不处于，则不允许发送数据，如果是幂等生产者，是允许
            if (!transactionManager.isSendToPartitionAllowed(tp))
                return true;

            // 获取PID和Epoch
            producerIdAndEpoch = transactionManager.producerIdAndEpoch();
            if (!producerIdAndEpoch.isValid())
                // PID和Epoch非法，返回true，直到我们有新的PID才允许数据发送
                return true;

            // 判断当前批次是否为重试批次
            if (!first.hasSequence()) {
                // 不是重试批次，判断「in-flight」是否有数据并且PID和Epoch是否是旧值
                // 简言之，就是「in-flight」中存在数据但PID和Epoch变更了，
                if (transactionManager.hasInflightBatches(tp) && transactionManager.hasStaleProducerIdAndEpoch(tp)) {
                    // 不会从「RecordAccumulator」抽取数据，否则一个新的PID和序号为0的消息批次将会发送给Broker而导致「out of sequence」错误
                    // 所以这里，应该等「in-flight」的数据全部收到ACK后，再使用新的PID和Epoch填充消息批次
                    return true;
                }

                // 判断该分区是否有未解决的序列号
                if (transactionManager.hasUnresolvedSequence(first.topicPartition))
                    // 如果前序序列号状态未知，那么就不要向该分区发送数据。
                    // 前序批次变为未知状态是因为消息批次在至少一次被发送到Broker，但由于过期超时被中止。
                    return true;
            }

            // 获取该分区处于「in-flight」第一个序列号
            int firstInFlightSequence = transactionManager.firstInFlightSequence(first.topicPartition);

            // 判断位于「in-flight」首个分区序号是否和入参的「firstInFlightSequence」相等
            // 如果不等，说明「in-flight」已经有一个批次正在重试。在这种情况下，我们就不会再从RecordAccumulator抽取消息批次。
            if (firstInFlightSequence != RecordBatch.NO_SEQUENCE        // 序列号存在
                    && first.hasSequence()                              //
                    && first.baseSequence() != firstInFlightSequence)   // 序列号不相等
                return true;
        }
        return false;
    }

    /**
     * 以批次为单位抽取发往单个节点的数据，总字节数不得超过 {@param maxSize}
     *
     * ① 遍历节点对应的所有Leader分区，根据分区从「发送缓冲区」获取对应的双端队列
     * ② 判断队列头部是否满足，如果不满足，则跳过该分区
     * ③ 长度是有限制，不得超出 {@param maxSize}
     * ④ 满足条件的批次将会被添加到「ready」集合中，且批次以后不可添加新的数据
     * ⑤ 每个分区只会准备一个可发送的批次，即一个Request请求最多只会包含一个分区消息批次
     *
     * @param cluster 集群元数据
     * @param node    ready node
     * @param maxSize 最大值，不得超过
     * @param now     时间戳
     * @return        待发送批次集合
     */
    private List<ProducerBatch> drainBatchesForOneNode(Cluster cluster, Node node, int maxSize, long now) {
        int size = 0;
        // #1 获取节点（node）所有的分区Leader
        List<PartitionInfo> parts = cluster.partitionsForNode(node.id());

        // #2 存储可发往节点的批次
        List<ProducerBatch> ready = new ArrayList<>();

        // #3 计算起始位置，使用「drainIndex」是避免分区饥饿，不要每次都从0开始
        int start = drainIndex = drainIndex % parts.size();
        do {
            // #4 从集合中获取某个分区
            PartitionInfo part = parts.get(drainIndex);
            TopicPartition tp = new TopicPartition(part.topic(), part.partition());

            // #5 计算下一个索引值
            this.drainIndex = (this.drainIndex + 1) % parts.size();

            // #6 如果分区被静音，则跳过。只能当需要保证分区顺序性的时候才会被静音
            if (isMuted(tp))
                continue;

            // #7 获取分区对应的双端队列，队列包含待发送的批次
            Deque<ProducerBatch> deque = getDeque(tp);
            if (deque == null)
                // 队列为空，跳过
                continue;

            // #8 共享变量，需上锁
            synchronized (deque) {
                // #8-1 先进先出
                ProducerBatch first = deque.peekFirst();
                if (first == null)
                    // 队列中没有数据，跳过
                    continue;

                // #8-2 判断批次否处于重试退避时间范围内
                boolean backoff = first.attempts() > 0 && first.waitedTimeMs(now) < retryBackoffMs;
                if (backoff)
                    // 仍处于重试退避时间范围内，则跳过当前分区
                    continue;

                // #8-3 总共累计的字节数不得超过「maxSize」
                if (size + first.estimatedSizeInBytes() > maxSize && !ready.isEmpty()) {
                    // 这里有一个罕见的情况，由于压缩，单个批次的大小大于设置的请求的最大值，
                    // 在这种情况下，我们仍然将以单个请求的形式发送这个批次（ready.isEmpty）
                    break;
                } else {
                    // #8-4 判断是否可以从RecordAccumulator中抽取消息批次
                    // 幂等生产者根据「in-flith」和Sequence Number 判断是否允许数据抽取
                    if (shouldStopDrainBatchesForPartition(first, tp))
                        // 不允许给当前分区抽取数据，跳出循环
                        break;

                    // 判断是否为事务型Producer
                    boolean isTransactional = transactionManager != null && transactionManager.isTransactional();
                    // 获取PID
                    ProducerIdAndEpoch producerIdAndEpoch = transactionManager != null ? transactionManager.producerIdAndEpoch() : null;

                    // 获取消息缓存队列
                    ProducerBatch batch = deque.pollFirst();
                    if (producerIdAndEpoch != null && !batch.hasSequence()) {
                        // 如果分区的PID/epoch和最新的生产者不匹配，我们更新并重置sequence numbers。
                        // 并将PID和EPOCH更新为最新值
                        transactionManager.maybeUpdateProducerIdAndEpoch(batch.topicPartition);

                        /**
                         * 设置PID+Sequence Number
                         * 如果消息批次已经拥有PID和序列号，我们不会更改这些值，因为可能会造成数据重复。
                         * 特别是，之前的尝试实际上可能已经被接受了，如果我们在这里改变生产者ID和序列，这个尝试也会被接受，造成重复。
                         * 此外，我们更新分区的下一个序列号，并让事务管理器跟踪该批次，以确保即使我们收到不符合顺序的响应，也能保持序列顺序。
                         */
                        batch.setProducerState(producerIdAndEpoch, transactionManager.sequenceNumber(batch.topicPartition), isTransactional);
                        // 增加序列号
                        transactionManager.incrementSequenceNumber(batch.topicPartition, batch.recordCount);
                        log.debug("Assigned producerId {} and producerEpoch {} to batch with base sequence " + "{} being sent to partition {}", producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, batch.baseSequence(), tp);
                        // 添加到「in-flight」集合中
                        transactionManager.addInFlightBatch(batch);
                    }
                    // #8-7 关闭当前批次添加功能，不允许再向当前批次添加任何消息
                    batch.close();

                    // #8-8 记录大小，保证不超过「maxSize」
                    size += batch.records().sizeInBytes();

                    // #8-9 加入List中
                    ready.add(batch);

                    // #8-10 更新批次抽取时间
                    batch.drained(now);
                }
            }
        } while (start != drainIndex); // 节点包含的Leader分区都挨个遍历后才退出while循环
        return ready;
    }

    /**
     * 抽取给定节点的所有数据，并将它们整理成一个批次列表，
     * 这些批次将适合每个节点的指定大小。
     * 这种方法试图避免反复选择相同的主题节点，导致其他节点处于饥饿状态。
     *
     * @param cluster 集群元数据
     * @param nodes   ready nodes
     * @param maxSize 每个节点允许抽取的最大字节数
     * @param now     当前的时间戳
     * @return        key：node id，value：将要发往该节点的所有批次
     */
    public Map<Integer, List<ProducerBatch>> drain(Cluster cluster, Set<Node> nodes, int maxSize, long now) {
        if (nodes.isEmpty())
            return Collections.emptyMap();

        Map<Integer, List<ProducerBatch>> batches = new HashMap<>();
        for (Node node : nodes) {
            List<ProducerBatch> ready = drainBatchesForOneNode(cluster, node, maxSize, now);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    /**
     * The earliest absolute time a batch will expire (in milliseconds)
     */
    public long nextExpiryTimeMs() {
        return this.nextBatchExpiryTimeMs;
    }

    private Deque<ProducerBatch> getDeque(TopicPartition tp) {
        return batches.get(tp);
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary.
     */
    private Deque<ProducerBatch> getOrCreateDeque(TopicPartition tp) {
        Deque<ProducerBatch> d = this.batches.get(tp);
        if (d != null)
            return d;
        d = new ArrayDeque<>();
        Deque<ProducerBatch> previous = this.batches.putIfAbsent(tp, d);
        if (previous == null)
            return d;
        else
            return previous;
    }

    /**
     * Deallocate the record batch
     */
    public void deallocate(ProducerBatch batch) {
        incomplete.remove(batch);
        // Only deallocate the batch if it is not a split batch because split batch are allocated outside the
        // buffer pool.
        if (!batch.isSplitBatch())
            free.deallocate(batch.buffer(), batch.initialCapacity());
    }

    /**
     * Package private for unit test. Get the buffer pool remaining size in bytes.
     */
    long bufferPoolAvailableMemory() {
        return free.availableMemory();
    }

    /**
     * Are there any threads currently waiting on a flush?
     *
     * package private for test
     */
    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    /* Visible for testing */
    Map<TopicPartition, Deque<ProducerBatch>> batches() {
        return Collections.unmodifiableMap(batches);
    }

    /**
     * Initiate the flushing of data from the accumulator...this makes all requests immediately ready
     */
    public void beginFlush() {
        this.flushesInProgress.getAndIncrement();
    }

    /**
     * Are there any threads currently appending messages?
     */
    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;
    }

    /**
     * Mark all partitions as ready to send and block until the send is complete
     */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            for (ProducerBatch batch : this.incomplete.copyAll())
                batch.produceFuture.await();
        } finally {
            this.flushesInProgress.decrementAndGet();
        }
    }

    /**
     * 检查是否还有未决的批次没有发送
     * @return true：存在未决批次没有发送，否则返回false
     */
    public boolean hasIncomplete() {
        return !this.incomplete.isEmpty();
    }

    /**
     * This function is only called when sender is closed forcefully. It will fail all the
     * incomplete batches and return.
     */
    public void abortIncompleteBatches() {
        // We need to keep aborting the incomplete batch until no thread is trying to append to
        // 1. Avoid losing batches.
        // 2. Free up memory in case appending threads are blocked on buffer full.
        // This is a tight loop but should be able to get through very quickly.
        do {
            abortBatches();
        } while (appendsInProgress());
        // After this point, no thread will append any messages because they will see the close
        // flag set. We need to do the last abort after no thread was appending in case there was a new
        // batch appended by the last appending thread.
        abortBatches();
        this.batches.clear();
    }

    /**
     * Go through incomplete batches and abort them.
     */
    private void abortBatches() {
        abortBatches(new KafkaException("Producer is closed forcefully."));
    }

    /**
     * Abort all incomplete batches (whether they have been sent or not)
     */
    void abortBatches(final RuntimeException reason) {
        for (ProducerBatch batch : incomplete.copyAll()) {
            Deque<ProducerBatch> dq = getDeque(batch.topicPartition);
            synchronized (dq) {
                batch.abortRecordAppends();
                dq.remove(batch);
            }
            batch.abort(reason);
            deallocate(batch);
        }
    }

    /**
     * Abort any batches which have not been drained
     */
    /**
     * 丢弃没有
     * @param reason
     */
    void abortUndrainedBatches(RuntimeException reason) {
        for (ProducerBatch batch : incomplete.copyAll()) {
            Deque<ProducerBatch> dq = getDeque(batch.topicPartition);
            boolean aborted = false;
            synchronized (dq) {
                if ((transactionManager != null && !batch.hasSequence()) || (transactionManager == null && !batch.isClosed())) {
                    aborted = true;
                    batch.abortRecordAppends();
                    dq.remove(batch);
                }
            }
            if (aborted) {
                batch.abort(reason);
                deallocate(batch);
            }
        }
    }

    public void mutePartition(TopicPartition tp) {
        muted.add(tp);
    }

    public void unmutePartition(TopicPartition tp) {
        muted.remove(tp);
    }

    /**
     * Close this accumulator and force all the record buffers to be drained
     */
    public void close() {
        this.closed = true;
        this.free.close();
    }

    /*
     * 一种元数据，用于记录一条消息添加到「消息累加器」的结果
     */
    public final static class RecordAppendResult {
        // 消息结果凭证
        public final FutureRecordMetadata future;
        // 批次是否满了
        public final boolean batchIsFull;
        // 是否有新批次被创建
        public final boolean newBatchCreated;
        // 被中断
        public final boolean abortForNewBatch;


        public RecordAppendResult(FutureRecordMetadata future, boolean batchIsFull, boolean newBatchCreated, boolean abortForNewBatch) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
            this.abortForNewBatch = abortForNewBatch;
        }
    }

    /*
     * 分区检查结果对象
     */
    public final static class ReadyCheckResult {
        // 该节点已经有准备好待发送的数据
        public final Set<Node> readyNodes;

        // 下次检查的延时时间
        public final long nextReadyCheckDelayMs;

        // Topic所属的Leader无元数据，需要重新获取
        public final Set<String> unknownLeaderTopics;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, Set<String> unknownLeaderTopics) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeaderTopics = unknownLeaderTopics;
        }
    }
}
