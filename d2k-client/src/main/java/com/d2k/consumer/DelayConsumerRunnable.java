/*
 * D2K - Delay to Kafka
 * Copyright (C) 2024 xiajuan96
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package com.d2k.consumer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 延迟消费者运行器
 * <p>
 * 负责从 Kafka 拉取消息并根据延迟配置进行延迟处理。
 * 支持基于消息头部的延迟时间和预配置的默认延迟时间。
 *
 * @param <K> 消息键类型
 * @param <V> 消息值类型
 * @author xiajuan96
 * @date 2025/8/8 16:37
 */
public class DelayConsumerRunnable<K, V> implements Runnable {

    /**
     * 日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(DelayConsumerRunnable.class);
    private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    /**
     * Kafka 消费者配置参数
     */
    private final Map<String, Object> kafkaConfigs;
    private final D2kConsumerConfig d2kConfig;


    /**
     * Kafka 集群地址
     */
    private final String bootstraps;
    /**
     * 订阅的主题列表
     */
    private final List<String> topics;

    /**
     * Kafka 消费者实例
     */
    private final Consumer<K, V> kafkaConsumer;

    /**
     * 分区延迟队列映射：TopicPartition -> 延迟消息优先队列
     */
    private final Map<TopicPartition, PriorityBlockingQueue<DelayItem<K, V>>> tpQueues = new ConcurrentHashMap<>();
    /**
     * 分区处理器映射：TopicPartition -> 分区处理器
     */
    private final Map<TopicPartition, TopicPartitionProcessor<K, V>> tpProcessors = new ConcurrentHashMap<>();
    /**
     * 待提交的偏移量映射：TopicPartition -> 下一个偏移量
     */
    private final Map<TopicPartition, Long> offsetsToCommit = new ConcurrentHashMap<>();
    /**
     * 已暂停的分区集合，用于流控管理
     */
    private final Set<TopicPartition> paused = ConcurrentHashMap.newKeySet();

    /**
     * 队列容量阈值，超过此值将暂停分区消费
     */
    private final int queueCapacityThreshold;

    /**
     * 主循环总耗时（毫秒），用于控制循环节奏
     */
    private final long loopTotalMillis;

    /**
     * 延迟消息处理器，用于处理到期的延迟消息
     */
    private final DelayItemHandler<K, V> delayItemHandler;

    /**
     * 异步处理配置
     */
    private final AsyncProcessingConfig asyncProcessingConfig;

    /**
     * 分区线程池映射：TopicPartition -> ExecutorService
     */
    private final Map<TopicPartition, ExecutorService> partitionExecutors = new ConcurrentHashMap<>();



    /**
     * 消费者运行状态标志
     */
    private boolean running;



    /**
     * 构造延迟消费者运行器（完整参数版本）
     *
     * @param configs               Kafka 消费者配置
     * @param topics                订阅的主题列表
     * @param delayItemHandler      延迟消息处理器
     * @param asyncProcessingConfig 异步处理配置
     */
    public DelayConsumerRunnable(Map<String, Object> configs,
                                 Collection<String> topics,
                                 DelayItemHandler<K, V> delayItemHandler,
                                 AsyncProcessingConfig asyncProcessingConfig) {
        // 参数校验 - 确保包含KafkaConsumer所需的全部必传参数
        validateRequiredConfigs(configs);
        
        // 分离Kafka原生配置和D2K专有配置
        Map<String, Object>[] separatedConfigs = separateConfigs(configs);
        this.kafkaConfigs = separatedConfigs[0];
        this.d2kConfig = new D2kConsumerConfig(separatedConfigs[1]);
        
        this.bootstraps = (String) kafkaConfigs.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        this.topics = new ArrayList<>(topics);
        this.delayItemHandler = delayItemHandler;
        this.asyncProcessingConfig = asyncProcessingConfig != null ? asyncProcessingConfig : AsyncProcessingConfig.createSyncConfig();
        this.loopTotalMillis = d2kConfig.getLoopTotalMs();
        this.queueCapacityThreshold = d2kConfig.getQueueCapacity();

        this.kafkaConsumer = buildKafkaConsumer(kafkaConfigs);
        kafkaConsumer.subscribe(topics, new DelayConsumerRebalanceListener());
    }

    private KafkaConsumer<K, V> buildKafkaConsumer(Map<String, Object> configs) {
        Map<String, Object> copy = new HashMap<>(configs);
        Object clientId = copy.get("client.id");
        if (clientId != null) {
            copy.put("client.id", clientId.toString() + CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement());
        }
        // 强制设置 enable.auto.commit 为 false，确保手动提交偏移量
        copy.put("enable.auto.commit", false);
        return new KafkaConsumer<>(copy);
    }

    /**
     * 兼容性构造函数（默认同步模式）
     */
    public DelayConsumerRunnable(Map<String, Object> configs,
                                 Collection<String> topics,
                                 DelayItemHandler<K, V> delayItemHandler) {
        this(configs, topics, delayItemHandler, AsyncProcessingConfig.createSyncConfig());
    }

    /**
     * 构造延迟消费者运行器（测试专用版本）
     * <p>
     * 便于测试：外部注入 Consumer（可为 MockConsumer）
     * 兼容性构造函数，测试专用，默认同步模式
     *
     * @param consumer         外部注入的 Kafka 消费者实例
     * @param topics           订阅的主题列表
     * @param delayItemHandler 延迟消息处理器
     */
    public DelayConsumerRunnable(Consumer<K, V> consumer,
                                 Collection<String> topics,
                                 DelayItemHandler<K, V> delayItemHandler) {
        this(consumer, topics, delayItemHandler, AsyncProcessingConfig.createSyncConfig());
    }

    /**
     * 构造延迟消费者运行器（测试专用版本）
     * <p>
     * 便于测试：外部注入 Consumer（可为 MockConsumer）
     *
     * @param consumer              外部注入的 Kafka 消费者实例
     * @param topics                订阅的主题列表
     * @param delayItemHandler      延迟消息处理器
     * @param asyncProcessingConfig 异步处理配置
     */
    public DelayConsumerRunnable(Consumer<K, V> consumer,
                                 Collection<String> topics,
                                 DelayItemHandler<K, V> delayItemHandler,
                                 AsyncProcessingConfig asyncProcessingConfig) {
        this.kafkaConfigs = Collections.emptyMap();
        this.d2kConfig = new D2kConsumerConfig();
        this.bootstraps = null;
        this.topics = new ArrayList<>(topics);
        this.delayItemHandler = delayItemHandler;
        this.asyncProcessingConfig = asyncProcessingConfig != null ? asyncProcessingConfig : AsyncProcessingConfig.createSyncConfig();
        this.loopTotalMillis = d2kConfig.getLoopTotalMs();
        this.queueCapacityThreshold = d2kConfig.getQueueCapacity();

        this.kafkaConsumer = consumer;
        kafkaConsumer.subscribe(topics, new DelayConsumerRebalanceListener());
    }



    /**
     * 运行延迟消费者主循环
     * <p>
     * 主要流程：
     * 1. 拉取消息前根据队列积压情况暂停分区
     * 2. 从 Kafka 拉取消息
     * 3. 将消息按分区分发到延迟队列
     * 4. 等待延迟处理完成
     * 5. 批量提交偏移量
     * 6. 根据队列积压情况恢复分区
     * 7. 补偿休眠以保持循环节奏
     */
    @Override
    public void run() {
        log.info("Starting DelayConsumerRunnable for topics: {}", topics);

        setRunning(true);
        while (isRunning()) {
            long loopStartNs = System.nanoTime();
            // 拉取消息前：仅根据 backlog 执行 pause，避免在拉取后再暂停
            adjustPauseByBacklogBeforePoll();
            // 循环拉取消息
            ConsumerRecords<K, V> records;
            try {
                log.debug("Polling for records with timeout 200ms");
                records = kafkaConsumer.poll(Duration.ofMillis(200L));
            } catch (WakeupException we) {
                log.debug("Received WakeupException");
                if (!running) {
                    log.info("Consumer is no longer running, breaking the loop");
                    break;
                } else {
                    continue;
                }
            }
            Set<TopicPartition> partitions = records.partitions();
            if (!records.isEmpty()) {
                log.debug("Received {} records from {} partitions", records.count(), partitions.size());
            }
            for (TopicPartition partition : partitions) {
                List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);
                log.debug("Processing {} records for partition {}-{}", partitionRecords.size(),
                        partition.topic(), partition.partition());
                //将不同partition的消息写入对应的阻塞队列中
                addDelayBufferQueue(partition, partitionRecords);
            }
            // 阻塞等待一小会，用于等待延迟处理delayBufferQueue中的数据。
            waitUntilTimeout();

            // 把不同partition的offset提交一下
            batchCommit();

            // 恢复 backlog 已降低的分区，便于异步拉取提前发生（不依赖是否拉到消息）
            log.debug("Adjusting resume by backlog before sleep");
            adjustResumeByBacklogBeforeSleep();

            // 如果本轮未拉到消息，补偿休眠：单轮总耗时 = loopTotalMillis
            if (records.isEmpty()) {
                long elapsedMs = (System.nanoTime() - loopStartNs) / 1_000_000L;
                long sleepMs = loopTotalMillis - elapsedMs;
                if (sleepMs > 0) {
                    try {
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

        }
        // 结束循环后，停止各queue的处理线程，再批量提交一下offset。
        log.info("Consumer loop ended, stopping queues");
        stopQueues();

        log.debug("Final batch commit");
        batchCommit();
        try {
            log.info("Closing Kafka consumer");
            kafkaConsumer.close();
        } catch (Exception e) {
            log.warn("Error closing Kafka consumer: {}", e.getMessage());
        }


    }

    /**
     * 停止所有分区的处理队列
     * <p>
     * 并行关闭所有 TopicPartitionProcessor 并清理相关资源，
     * 避免串行关闭导致的累积等待时间过长问题。
     */
    private void stopQueues() {
        stopQueues(10, 3, 5);
    }

    /**
     * 停止所有分区的处理队列（可配置超时时间）
     *
     * @param gracefulTimeoutSeconds 优雅关闭等待时间（秒）
     * @param forceTimeoutSeconds    强制关闭等待时间（秒）
     * @param taskTimeoutSeconds     任务完成等待时间（秒）
     */
    private void stopQueues(long gracefulTimeoutSeconds, long forceTimeoutSeconds, long taskTimeoutSeconds) {
        log.debug("Stopping {} topic partition processors in parallel", tpProcessors.size());

        if (tpProcessors.isEmpty()) {
            return;
        }

        // 创建并行关闭任务列表
        List<CompletableFuture<Void>> shutdownFutures = new ArrayList<>();

        for (Map.Entry<TopicPartition, TopicPartitionProcessor<K, V>> entry : tpProcessors.entrySet()) {
            TopicPartition tp = entry.getKey();
            TopicPartitionProcessor<K, V> processor = entry.getValue();

            // 为每个处理器创建异步关闭任务
            CompletableFuture<Void> shutdownFuture = CompletableFuture.runAsync(() -> {
                try {
                    log.debug("Shutting down processor for {}-{}", tp.topic(), tp.partition());
                    processor.shutdown(gracefulTimeoutSeconds, forceTimeoutSeconds, taskTimeoutSeconds);
                    log.debug("Successfully shut down processor for {}-{}", tp.topic(), tp.partition());
                } catch (Exception e) {
                    log.warn("Error shutting down processor for {}-{}: {}",
                            tp.topic(), tp.partition(), e.getMessage(), e);
                }
            });

            shutdownFutures.add(shutdownFuture);
        }

        // 等待所有关闭任务完成
        try {
            long totalTimeoutSeconds = gracefulTimeoutSeconds + forceTimeoutSeconds + taskTimeoutSeconds + 5; // 额外5秒缓冲
            CompletableFuture<Void> allShutdowns = CompletableFuture.allOf(
                    shutdownFutures.toArray(new CompletableFuture[0]));

            allShutdowns.get(totalTimeoutSeconds, TimeUnit.SECONDS);
            log.debug("All {} processors shut down successfully", tpProcessors.size());

        } catch (TimeoutException e) {
            log.warn("Timeout waiting for all processors to shut down, some may still be running");
        } catch (Exception e) {
            log.warn("Error waiting for processors to shut down: {}", e.getMessage(), e);
        }

        // 清理资源
        tpProcessors.clear();
        tpQueues.clear();
    }

    /**
     * 批量提交偏移量
     * <p>
     * 将所有待提交的偏移量一次性提交到 Kafka，
     * 提交成功后清理已提交的偏移量记录。
     */
    private void batchCommit() {
        if (offsetsToCommit.isEmpty()) {
            return;
        }
        log.debug("Batch committing offsets for {} partitions", offsetsToCommit.size());
        Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> e : offsetsToCommit.entrySet()) {
            log.debug("Committing offset {} for {}-{}", e.getValue(),
                    e.getKey().topic(), e.getKey().partition());
            commitMap.put(e.getKey(), new OffsetAndMetadata(e.getValue()));
        }
        try {
            kafkaConsumer.commitSync(commitMap);
            offsetsToCommit.keySet().removeAll(commitMap.keySet());
            log.debug("Successfully committed offsets for {} partitions", commitMap.size());
        } catch (Exception e) {
            log.error("Error committing offsets: {}", e.getMessage());
        }
    }

    /**
     * 等待延迟处理完成
     * <p>
     * 检查所有延迟队列状态：
     * - 如果所有队列都为空，休眠 50ms
     * - 如果有队列非空，休眠 20ms 等待处理
     */
    private void waitUntilTimeout() {
        //当所有的阻塞队列都为空 or 等到超时时间后，就结束等待
        boolean allEmpty = true;
        for (PriorityBlockingQueue<DelayItem<K, V>> q : tpQueues.values()) {
            if (!q.isEmpty()) {
                allEmpty = false;
                break;
            }
        }
        try {
            if (allEmpty) {
                Thread.sleep(50L);
            } else {
                Thread.sleep(20L);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 将分区消息添加到延迟缓冲队列
     * <p>
     * 为每个分区创建或获取对应的延迟队列，并将消息按延迟时间排序加入队列。
     * 如果分区队列不存在，会自动创建队列和对应的处理器。
     *
     * @param partition        主题分区
     * @param partitionRecords 该分区的消息记录列表
     */
    private void addDelayBufferQueue(TopicPartition partition, List<ConsumerRecord<K, V>> partitionRecords) {
        log.debug("Adding {} records to delay buffer queue for {}-{}",
                partitionRecords.size(), partition.topic(), partition.partition());
        PriorityBlockingQueue<DelayItem<K, V>> queue = tpQueues.computeIfAbsent(partition, tp -> {
            log.debug("Creating new queue for {}-{}", tp.topic(), tp.partition());
            PriorityBlockingQueue<DelayItem<K, V>> q = new PriorityBlockingQueue<>();
            startProcessorIfAbsent(tp, q);
            return q;
        });
        for (ConsumerRecord<K, V> record : partitionRecords) {
            long resumeAt = extractResumeAtMillis(record, partition);
            long delayMs = Math.max(0L, resumeAt - System.currentTimeMillis());
            log.debug("Adding record with key={}, offset={}, delay={}ms, resumeAt={} to queue",
                    record.key(), record.offset(), delayMs, resumeAt);
            queue.offer(new DelayItem<>(delayMs, resumeAt, record));
        }
    }

    /**
     * 执行一次消息处理循环
     * <p>
     * 用于测试或单次处理场景，执行一次消息拉取、分发和提交操作。
     */
    public void processOnce() {
        ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofMillis(50L));
        Set<TopicPartition> partitions = records.partitions();
        for (TopicPartition partition : partitions) {
            addDelayBufferQueue(partition, records.records(partition));
        }
        batchCommit();
    }


    /**
     * 检查消费者是否正在运行
     *
     * @return true 如果消费者正在运行，false 否则
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * 设置消费者运行状态
     *
     * @param running 运行状态，true 表示运行，false 表示停止
     */
    public void setRunning(boolean running) {
        this.running = running;
    }

    /**
     * 延迟消费者重平衡监听器
     * <p>
     * 处理 Kafka 消费者组重平衡事件，管理分区分配和回收时的资源清理。
     */
    class DelayConsumerRebalanceListener implements ConsumerRebalanceListener {

        /**
         * 分区被回收时的处理
         * <p>
         * 停止被回收分区的处理器，清理相关队列，并提交已处理的偏移量。
         *
         * @param partitions 被回收的分区集合
         */
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.info("Partitions revoked: {}", partitions);

            if (partitions.isEmpty()) {
                return;
            }

            // 并行关闭被回收分区的处理器
            List<CompletableFuture<Void>> shutdownFutures = new ArrayList<>();

            for (TopicPartition tp : partitions) {
                TopicPartitionProcessor<K, V> processor = tpProcessors.get(tp);
                if (processor != null) {
                    // 为每个处理器创建异步关闭任务
                    CompletableFuture<Void> shutdownFuture = CompletableFuture.runAsync(() -> {
                        try {
                            log.debug("Shutting down processor for revoked partition {}-{}", tp.topic(), tp.partition());
                            processor.shutdown(5, 2, 3, 2); // 使用较短的超时时间和缓冲时间，适应rebalance场景
                            log.debug("Successfully shut down processor for revoked partition {}-{}", tp.topic(), tp.partition());
                        } catch (Exception e) {
                            log.warn("Error shutting down processor for revoked partition {}-{}: {}",
                                    tp.topic(), tp.partition(), e.getMessage(), e);
                        }
                    });
                    shutdownFutures.add(shutdownFuture);
                }
            }

            // 等待所有关闭任务完成
            if (!shutdownFutures.isEmpty()) {
                try {
                    CompletableFuture<Void> allShutdowns = CompletableFuture.allOf(
                            shutdownFutures.toArray(new CompletableFuture[0]));
                    allShutdowns.get(15, TimeUnit.SECONDS); // 总超时时间
                    log.debug("All revoked partition processors shut down successfully");
                } catch (TimeoutException e) {
                    log.warn("Timeout waiting for revoked partition processors to shut down");
                } catch (Exception e) {
                    log.warn("Error waiting for revoked partition processors to shut down: {}", e.getMessage(), e);
                }
            }

            // 清理资源
            for (TopicPartition tp : partitions) {
                tpProcessors.remove(tp);
                tpQueues.remove(tp);

                // 移除线程池引用，实际关闭由TopicPartitionProcessor的shutdown方法负责
                partitionExecutors.remove(tp);
                log.debug("Cleaned up resources for revoked partition {}-{}", tp.topic(), tp.partition());
            }

            batchCommit();
        }

        /**
         * 分区被分配时的处理
         * <p>
         * 为新分配的分区创建延迟队列和处理器。
         *
         * @param partitions 新分配的分区集合
         */
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.info("Partitions assigned: {}", partitions);
            for (TopicPartition tp : partitions) {
                log.debug("Setting up processor for newly assigned partition {}-{}",
                        tp.topic(), tp.partition());
                PriorityBlockingQueue<DelayItem<K, V>> q = tpQueues.computeIfAbsent(tp, k -> new PriorityBlockingQueue<>());
                startProcessorIfAbsent(tp, q);
            }
        }
    }

    /**
     * 为分区启动处理器（如果不存在）
     * <p>
     * 为指定的主题分区创建并启动 TopicPartitionProcessor，
     * 如果处理器已存在则不重复创建。
     *
     * @param tp    主题分区
     * @param queue 该分区对应的延迟队列
     */
    private void startProcessorIfAbsent(TopicPartition tp, PriorityBlockingQueue<DelayItem<K, V>> queue) {
        tpProcessors.computeIfAbsent(tp, k -> {
            log.debug("Starting new processor for {}-{}", tp.topic(), tp.partition());

            // 根据AsyncProcessingConfig为每个分区创建独立的线程池
            ExecutorService executor = createExecutorForPartition(tp);

            TopicPartitionProcessor<K, V> p = new TopicPartitionProcessor<>(queue, delayItemHandler, tp,
                    (topicPartition, nextOffset) -> {
                        log.debug("Recording success for {}-{} with offset {}",
                                topicPartition.topic(), topicPartition.partition(), nextOffset);
                        offsetsToCommit.put(topicPartition, nextOffset);
                    }, executor);
            p.start();
            return p;
        });
    }

    /**
     * 为指定分区创建线程池
     *
     * @param tp 主题分区
     * @return 线程池实例，如果配置为同步模式则返回null
     */
    private ExecutorService createExecutorForPartition(TopicPartition tp) {
        if (!asyncProcessingConfig.isEnabled()) {
            return null;
        }

        ExecutorService executor = new ThreadPoolExecutor(
                asyncProcessingConfig.getCorePoolSize(),
                asyncProcessingConfig.getMaximumPoolSize(),
                asyncProcessingConfig.getKeepAliveTime(),
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(asyncProcessingConfig.getQueueCapacity()),
                r -> new Thread(r, "DelayProcessor-" + tp.topic() + "-" + tp.partition()),
                asyncProcessingConfig.toRejectedExecutionHandler());

        // 将线程池存储到映射中，便于后续管理
        partitionExecutors.put(tp, executor);

        return executor;
    }



    /**
     * 在拉取消息前根据队列积压情况暂停分区
     * <p>
     * 仅处理暂停操作，避免在拉取消息后再暂停分区。
     * 如果队列大小超过阈值且未暂停，则暂停该分区。
     */
    private void adjustPauseByBacklogBeforePoll() {
        Set<TopicPartition> toPause = new HashSet<>();
        for (Map.Entry<TopicPartition, PriorityBlockingQueue<DelayItem<K, V>>> e : tpQueues.entrySet()) {
            TopicPartition tp = e.getKey();
            int size = e.getValue().size();
            if (size > queueCapacityThreshold && !paused.contains(tp)) {
                toPause.add(tp);
            }
        }
        if (!toPause.isEmpty()) {
            kafkaConsumer.pause(toPause);
            paused.addAll(toPause);
        }
    }

    /**
     * 在补偿休眠前根据队列积压情况恢复分区
     * <p>
     * 仅处理恢复操作，便于异步拉取提前发生。
     * 如果队列大小低于阈值且已暂停，则恢复该分区。
     */
    private void adjustResumeByBacklogBeforeSleep() {
        Set<TopicPartition> toResume = new HashSet<>();
        for (Map.Entry<TopicPartition, PriorityBlockingQueue<DelayItem<K, V>>> e : tpQueues.entrySet()) {
            TopicPartition tp = e.getKey();
            int size = e.getValue().size();
            double usage = (double) size / queueCapacityThreshold;
            if (usage <= 1.0 && paused.contains(tp)) {
                toResume.add(tp);
            }
        }
        if (!toResume.isEmpty()) {
            kafkaConsumer.resume(toResume);
            paused.removeAll(toResume);
        }
    }



    /**
     * 获取Kafka原生配置
     * @return Kafka配置的只读副本
     */
    public Map<String, Object> getKafkaConfigs() {
        return Collections.unmodifiableMap(kafkaConfigs);
    }

    /**
     * 获取D2K专有配置
     * @return D2K配置的只读副本
     */
    public Map<String, Object> getD2kConfigs() {
        return d2kConfig.getOriginalConfigs();
    }
    
    /**
     * 获取D2K消费者配置对象
     * 
     * @return D2K消费者配置对象
     */
    public D2kConsumerConfig getD2kConfig() {
        return d2kConfig;
    }

    /**
     * 校验必传配置参数
     *
     * @param configs Kafka消费者配置
     * @throws IllegalArgumentException 如果缺少必传参数或参数格式不正确
     */
    private static void validateRequiredConfigs(Map<String, Object> configs) {
        if (configs == null) {
            throw new IllegalArgumentException("配置参数不能为null");
        }
        
        // 检查必传参数
        String[] requiredParams = {
            "bootstrap.servers",
            "group.id", 
            "key.deserializer",
            "value.deserializer"
        };
        
        for (String param : requiredParams) {
            Object value = configs.get(param);
            if (value == null) {
                throw new IllegalArgumentException("缺少必传参数: " + param);
            }
            
            String stringValue = value.toString().trim();
            if (stringValue.isEmpty()) {
                throw new IllegalArgumentException("参数值不能为空: " + param);
            }
            
            // 特殊校验
            if ("bootstrap.servers".equals(param)) {
                validateBootstrapServers(stringValue, param);
            } else if (param.endsWith(".deserializer")) {
                validateDeserializerClass(stringValue, param);
            }
        }
    }
    
    /**
     * 校验bootstrap.servers参数格式
     */
    private static void validateBootstrapServers(String servers, String paramName) {
        String[] serverList = servers.split(",");
        for (String server : serverList) {
            server = server.trim();
            if (!server.contains(":")) {
                throw new IllegalArgumentException(paramName + "格式错误，应为host:port格式，当前值: " + server);
            }
            
            String[] parts = server.split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException(paramName + "格式错误，应为host:port格式，当前值: " + server);
            }
            
            try {
                int port = Integer.parseInt(parts[1]);
                if (port <= 0 || port > 65535) {
                    throw new IllegalArgumentException(paramName + "端口号无效，应在1-65535范围内，当前值: " + port);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(paramName + "端口号格式错误，应为数字，当前值: " + parts[1]);
            }
        }
    }
    
    /**
     * 校验反序列化器类名
     */
    private static void validateDeserializerClass(String className, String paramName) {
        try {
            Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(paramName + "指定的类不存在: " + className);
        }
    }

    /**
     * 分离Kafka原生配置和D2K专有配置
     * @param configs 原始配置映射
     * @return 数组，[0]为Kafka配置，[1]为D2K配置
     */
    private static Map<String, Object>[] separateConfigs(Map<String, Object> configs) {
        Map<String, Object> kafkaConfigs = new HashMap<>();
        Map<String, Object> d2kConfigs = D2kConsumerConfig.extractD2kConfigs(configs);
        
        for (Map.Entry<String, Object> entry : configs.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith("d2k.")) {
                // Kafka原生配置
                kafkaConfigs.put(key, entry.getValue());
            }
        }
        
        @SuppressWarnings("unchecked")
        Map<String, Object>[] result = new Map[2];
        result[0] = kafkaConfigs;
        result[1] = d2kConfigs;
        return result;
    }

    /**
     * 提取消息的恢复处理时间戳
     * <p>
     * 按优先级顺序确定消息的处理时间：
     * 1. 优先使用 d2k-deliver-at 头部（绝对时间戳）
     * 2. 其次使用 d2k-delay-ms 头部（相对延迟时间）
     * 3. 默认为立即处理（0 延迟）
     *
     * @param record 消息记录
     * @param tp     主题分区
     * @return 消息应该被处理的时间戳（毫秒）
     */
    private long extractResumeAtMillis(ConsumerRecord<K, V> record, TopicPartition tp) {
        Long deliverAt = headerAsLong(record, "d2k-deliver-at");
        if (deliverAt != null && deliverAt > 0) {
            log.debug("Found d2k-deliver-at header: {}", deliverAt);
            return deliverAt;
        }

        Long delayMs = headerAsLong(record, "d2k-delay-ms");
        if (delayMs == null) {
            delayMs = 0L;
            log.debug("No delay header found, using default delay: {}ms for {}-{}",
                    delayMs, tp.topic(), tp.partition());
        } else {
            log.debug("Found d2k-delay-ms header: {}ms", delayMs);
        }

        long baseTs = record.timestamp();
        long resumeAt = baseTs + Math.max(0L, delayMs);
        log.debug("Calculated resumeAt={} (baseTs={} + delayMs={})", resumeAt, baseTs, delayMs);
        return resumeAt;
    }

    /**
     * 从消息头部提取长整型值
     * <p>
     * 安全地从消息头部获取指定键的长整型值，支持类型转换和异常处理。
     *
     * @param record 消息记录
     * @param key    头部键名
     * @return 头部值转换为长整型，如果不存在或转换失败则返回 null
     */
    private Long headerAsLong(ConsumerRecord<K, V> record, String key) {
        if (record.headers() == null) return null;
        Header h = record.headers().lastHeader(key);
        if (h == null) return null;
        try {
            return Long.parseLong(new String(h.value(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 记录消息处理成功
     * <p>
     * 供 TopicPartitionProcessor 回调使用，记录成功处理的消息偏移量。
     * 将下一个偏移量标记为待提交状态。
     *
     * @param record 成功处理的消息记录
     */
    public void recordSuccess(ConsumerRecord<K, V> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        long nextOffset = record.offset() + 1;
        log.debug("Recording success for record topic={}, partition={}, offset={}, next offset={}",
                record.topic(), record.partition(), record.offset(), nextOffset);
        offsetsToCommit.put(tp, nextOffset);
    }

    /**
     * 关闭延迟消费者
     * <p>
     * 设置运行状态为 false 并唤醒消费者，触发主循环退出。
     * 这是一个优雅关闭方法，不会立即中断正在处理的消息。
     * 通过调用stopQueues来关闭处理器，由处理器负责线程池的清理。
     */
    public void shutdown() {
        log.info("Shutting down DelayConsumerRunnable");
        this.running = false;
        try {
            kafkaConsumer.wakeup();
        } catch (Exception e) {
            log.warn("Error during consumer wakeup: {}", e.getMessage());
        }

        // 关闭所有分区的处理器，由处理器负责线程池清理
        stopQueues();

        // 清理线程池引用
        partitionExecutors.clear();
    }
}
