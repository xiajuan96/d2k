package com.d2k.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CountDownLatch;

/**
 *
 * @author xiajuan96
 * @date 2025/8/13 15:53
 */
public class TopicPartitionProcessor<K, V> extends Thread {

    private static final Logger log = LoggerFactory.getLogger(TopicPartitionProcessor.class);

    private final PriorityBlockingQueue<DelayItem<K, V>> queue;
    private final DelayItemHandler<K, V> delayItemHandler;
    private final TopicPartition topicPartition;
    private final BiConsumer<TopicPartition, Long> commitCallback;
    
    // 异步处理相关字段（仅在启用异步模式时使用）
    private final ExecutorService processingExecutor;
    private final ConcurrentHashMap<Long, CompletableFuture<Void>> pendingTasks;
    private final AtomicLong nextExpectedOffset;
    private final ConcurrentSkipListMap<Long, DelayItem<K, V>> completedItems;
    
    // 用于同步shutdown等待资源清理完成的信号量
    private final CountDownLatch cleanupCompleteLatch = new CountDownLatch(1);
    
    private boolean running;

    public TopicPartitionProcessor(PriorityBlockingQueue<DelayItem<K, V>> queue,
                                   DelayItemHandler<K, V> delayItemHandler,
                                   TopicPartition topicPartition,
                                   BiConsumer<TopicPartition, Long> commitCallback,
                                   ExecutorService processingExecutor) {
        this.queue = queue;
        this.delayItemHandler = delayItemHandler;
        this.topicPartition = topicPartition;
        this.commitCallback = commitCallback;
        this.processingExecutor = processingExecutor;
        
        // 根据线程池是否为null决定是否初始化异步处理相关字段
        if (this.processingExecutor != null) {
            // 异步模式：初始化异步处理相关字段
            this.pendingTasks = new ConcurrentHashMap<>();
            this.nextExpectedOffset = new AtomicLong(-1);
            this.completedItems = new ConcurrentSkipListMap<>();
        } else {
            // 同步模式：不需要这些字段
            this.pendingTasks = null;
            this.nextExpectedOffset = null;
            this.completedItems = null;
        }
    }
    
    /**
     * 兼容性构造函数（默认同步模式）
     */
    public TopicPartitionProcessor(PriorityBlockingQueue<DelayItem<K, V>> queue,
                                   DelayItemHandler<K, V> delayItemHandler,
                                   TopicPartition topicPartition,
                                   BiConsumer<TopicPartition, Long> commitCallback) {
        this(queue, delayItemHandler, topicPartition, commitCallback, null);
    }

    @Override
    public void run() {
        running = true;
        log.debug("Starting TopicPartitionProcessor for {}-{}", topicPartition.topic(), topicPartition.partition());
        
        try {
            while (running) {
                try {
                    processNextDelayItem();
                } catch (InterruptedException ie) {
                    handleInterruption();
                    break;
                } catch (Exception e) {
                    handleProcessingError(e);
                }
            }
        } finally {
            // 确保在线程结束时清理资源
            log.debug("TopicPartitionProcessor for {}-{} stopped, cleaning up resources", topicPartition.topic(), topicPartition.partition());
            try {
                cleanupResources(30, 5, 10); // 使用默认超时时间
            } finally {
                // 无论清理是否成功，都要释放信号量，通知等待的shutdown方法
                running = false;
                cleanupCompleteLatch.countDown();
                log.debug("Resource cleanup completed for {}-{}, signaling shutdown waiters", topicPartition.topic(), topicPartition.partition());
            }
        }
    }

    /**
     * 处理下一个延迟消息项
     * 
     * @throws InterruptedException 当线程被中断时抛出
     */
    private void processNextDelayItem() throws InterruptedException {
        DelayItem<K, V> item = waitForReadyItem();
        if (item != null) {
            executeDelayItem(item);
            // 注意：偏移量提交已经在executeDelayItem中处理
            // - 同步模式：executeDelayItemSync中直接调用commitItemOffset
            // - 异步模式：executeDelayItemAsync中通过tryCommitSequentialOffsets处理
        }
    }

    /**
     * 等待并获取准备就绪的延迟消息项
     * 
     * @return 准备就绪的延迟消息项，如果队列为空或消息未到期则返回 null
     * @throws InterruptedException 当线程被中断时抛出
     */
    private DelayItem<K, V> waitForReadyItem() throws InterruptedException {
        DelayItem<K, V> head = queue.peek();
        if (head == null) {
            sleepWhenQueueEmpty();
            return null;
        }
        
        if (!isItemReady(head)) {
            sleepUntilItemReady(head);
            return null;
        }
        
        DelayItem<K, V> item = queue.poll();
        if (item == null) {
            log.debug("Failed to poll item from queue for {}-{}", topicPartition.topic(), topicPartition.partition());
            return null;
        }
        
        log.debug("Retrieved item from queue for {}-{}, offset={}, delay={}ms", 
                topicPartition.topic(), topicPartition.partition(), 
                item.getRecord().offset(), item.getResumeAtTimestamp() - System.currentTimeMillis());
        return item;
    }

    /**
     * 当队列为空时休眠
     * 
     * @throws InterruptedException 当线程被中断时抛出
     */
    private void sleepWhenQueueEmpty() throws InterruptedException {
        log.debug("Queue empty for {}-{}, sleeping 50ms", topicPartition.topic(), topicPartition.partition());
        TimeUnit.MILLISECONDS.sleep(50L);
    }

    /**
     * 检查延迟消息项是否准备就绪
     * 
     * @param item 延迟消息项
     * @return true 如果消息已到期，false 否则
     */
    private boolean isItemReady(DelayItem<K, V> item) {
        long resumeAtTimestamp = item.getResumeAtTimestamp();
        long now = System.currentTimeMillis();
        return resumeAtTimestamp <= now;
    }

    /**
     * 等待直到延迟消息项准备就绪
     * 
     * 采用分段休眠策略：
     * - 计算消息到期的剩余时间 remainMillis
     * - 使用 Math.min(remainMillis, 200L) 限制单次休眠时间不超过 200ms
     * - 这种设计的精妙之处在于：
     *   1. 避免长时间休眠导致的响应延迟（如果消息延迟时间很长）
     *   2. 提高线程的响应性，能够及时响应中断信号
     *   3. 在保证精确延迟的同时，维持合理的 CPU 使用率
     *   4. 当有新的更早到期的消息加入队列时，能够更快地重新调度
     * 
     * @param item 延迟消息项
     * @throws InterruptedException 当线程被中断时抛出
     */
    private void sleepUntilItemReady(DelayItem<K, V> item) throws InterruptedException {
        long resumeAtTimestamp = item.getResumeAtTimestamp();
        long now = System.currentTimeMillis();
        long remainMillis = resumeAtTimestamp - now;
        // 限制单次休眠时间不超过200ms，平衡延迟精度和响应性
        long sleepTime = Math.min(remainMillis, 200L);
        
        log.debug("Item not ready yet for {}-{}, waiting {}ms (resumeAt={}, now={})", 
                topicPartition.topic(), topicPartition.partition(), 
                sleepTime, resumeAtTimestamp, now);
        TimeUnit.MILLISECONDS.sleep(sleepTime);
    }

    /**
     * 执行延迟消息项的业务逻辑
     * 
     * 根据线程池是否存在选择同步或异步处理模式：
     * - 同步模式：直接在当前线程中处理消息（线程池为null）
     * - 异步模式：使用线程池异步处理消息，避免长时间运行的消息阻塞后续消息处理
     * 
     * @param item 延迟消息项
     */
    private void executeDelayItem(DelayItem<K, V> item) {
        if (processingExecutor != null) {
            executeDelayItemAsync(item);
        } else {
            executeDelayItemSync(item);
        }
    }
    
    /**
     * 同步执行延迟消息项的业务逻辑
     * 
     * @param item 延迟消息项
     */
    private void executeDelayItemSync(DelayItem<K, V> item) {
        ConsumerRecord<K, V> record = item.getRecord();
        
        // 记录处理开始
        if (log.isInfoEnabled()) {
            log.info("processing record topic={} partition={} offset={} dueAt={} now={}",
                    topicPartition.topic(), topicPartition.partition(),
                    record.offset(), item.getResumeAtTimestamp(), System.currentTimeMillis());
        }
        
        try {
            log.debug("Calling delayItemHandler.process for record with key={}, offset={}", 
                    record.key(), record.offset());
            delayItemHandler.process(item);
            
            // 记录处理完成
            if (log.isInfoEnabled()) {
                log.info("processed record topic={} partition={} offset={} committedNext={}",
                        topicPartition.topic(), topicPartition.partition(),
                        record.offset(), record.offset() + 1);
            }
            
            // 直接提交偏移量
            commitItemOffset(item);
            
        } catch (Exception e) {
            log.error("Error processing record topic={} partition={} offset={}: {}", 
                    topicPartition.topic(), topicPartition.partition(), record.offset(), e.getMessage(), e);
            // 同步模式下，处理失败也提交偏移量，避免重复消费
            commitItemOffset(item);
        }
    }
    
    /**
     * 异步执行延迟消息项的业务逻辑
     * 
     * 使用线程池异步处理消息，避免长时间运行的消息阻塞后续消息处理。
     * 同时维护偏移量的顺序性，确保消息按顺序提交。
     * 
     * @param item 延迟消息项
     */
    private void executeDelayItemAsync(DelayItem<K, V> item) {
        ConsumerRecord<K, V> record = item.getRecord();
        long offset = record.offset();
        
        // 初始化期望的下一个偏移量
        if (nextExpectedOffset.get() == -1) {
            nextExpectedOffset.set(offset);
        }
        
        // 记录处理开始
        if (log.isInfoEnabled()) {
            log.info("submitting async processing for record topic={} partition={} offset={} dueAt={} now={}",
                    topicPartition.topic(), topicPartition.partition(),
                    offset, item.getResumeAtTimestamp(), System.currentTimeMillis());
        }
        
        // 异步处理消息
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            try {
                log.debug("Calling delayItemHandler.process for record with key={}, offset={}", 
                        record.key(), offset);
                delayItemHandler.process(item);
                
                if (log.isInfoEnabled()) {
                    log.info("completed async processing for record topic={} partition={} offset={}",
                            topicPartition.topic(), topicPartition.partition(), offset);
                }
            } catch (Exception e) {
                log.error("Error processing record topic={} partition={} offset={}: {}", 
                        topicPartition.topic(), topicPartition.partition(), offset, e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }, processingExecutor);
        
        // 处理完成后的回调
        future.whenComplete((result, throwable) -> {
            pendingTasks.remove(offset);
            if (throwable == null) {
                // 处理成功，加入已完成队列
                completedItems.put(offset, item);
                // 尝试提交连续的偏移量
                tryCommitSequentialOffsets();
            } else {
                log.error("Failed to process record topic={} partition={} offset={}", 
                        topicPartition.topic(), topicPartition.partition(), offset, throwable);
                // 处理失败的策略可以根据业务需求调整
                // 这里选择跳过失败的消息，继续处理后续消息
                completedItems.put(offset, item);
                tryCommitSequentialOffsets();
            }
        });
        
        // 记录待处理任务
        pendingTasks.put(offset, future);
    }

    /**
     * 提交消息项的偏移量（同步处理时使用）
     * 
     * @param item 延迟消息项
     */
    private void commitItemOffset(DelayItem<K, V> item) {
        ConsumerRecord<K, V> record = item.getRecord();
        long nextOffset = record.offset() + 1;
        
        log.debug("Calling commitCallback for {}-{} with offset {}", 
                topicPartition.topic(), topicPartition.partition(), nextOffset);
        commitCallback.accept(topicPartition, nextOffset);
    }

    /**
     * 尝试提交连续的偏移量
     * 
     * 确保偏移量按顺序提交，避免消息重复消费。
     * 只有当前期望的偏移量及其后续连续偏移量都已完成处理时，才进行提交。
     */
    private synchronized void tryCommitSequentialOffsets() {
        long expectedOffset = nextExpectedOffset.get();
        
        // 查找连续已完成的偏移量
        while (completedItems.containsKey(expectedOffset)) {
            DelayItem<K, V> completedItem = completedItems.remove(expectedOffset);
            
            // 提交偏移量
            long nextOffset = expectedOffset + 1;
            log.debug("Committing sequential offset for {}-{} with offset {}", 
                    topicPartition.topic(), topicPartition.partition(), nextOffset);
            
            if (log.isInfoEnabled()) {
                log.info("processed record topic={} partition={} offset={} committedNext={}",
                        topicPartition.topic(), topicPartition.partition(),
                        expectedOffset, nextOffset);
            }
            
            commitCallback.accept(topicPartition, nextOffset);
            
            // 更新期望的下一个偏移量
            expectedOffset++;
            nextExpectedOffset.set(expectedOffset);
        }
    }

    /**
     * 处理线程中断
     */
    private void handleInterruption() {
        log.debug("TopicPartitionProcessor for {}-{} interrupted", topicPartition.topic(), topicPartition.partition());
    }

    /**
     * 处理处理过程中的异常
     * 
     * @param e 异常对象
     */
    private void handleProcessingError(Exception e) {
        // 完善异常处理机制，保持线程存活并记录详细错误信息
        log.error("processor error on {}-{}: {}", topicPartition.topic(), topicPartition.partition(), e.getMessage(), e);
        // 可以考虑添加重试机制或发送告警通知
        try {
            TimeUnit.MILLISECONDS.sleep(1000); // 短暂休眠避免频繁错误日志
        } catch (InterruptedException ie) {
            log.debug("Sleep interrupted during error handling for {}-{}", topicPartition.topic(), topicPartition.partition());
            Thread.currentThread().interrupt(); // 恢复中断状态
        }
    }


    
    /**
     * 请求关闭处理器（可配置超时时间）
     * 设置停止标志、中断主线程，并等待资源清理完成
     * 
     * @param gracefulTimeoutSeconds 优雅关闭等待时间（秒）
     * @param forceTimeoutSeconds 强制关闭等待时间（秒）
     * @param taskTimeoutSeconds 任务完成等待时间（秒）
     */
    public void shutdown(long gracefulTimeoutSeconds, long forceTimeoutSeconds, long taskTimeoutSeconds) {
        shutdown(gracefulTimeoutSeconds, forceTimeoutSeconds, taskTimeoutSeconds, 10);
    }
    
    /**
     * 请求关闭处理器（可配置超时时间和缓冲时间）
     * 设置停止标志、中断主线程，并等待资源清理完成
     * 
     * @param gracefulTimeoutSeconds 优雅关闭等待时间（秒）
     * @param forceTimeoutSeconds 强制关闭等待时间（秒）
     * @param taskTimeoutSeconds 任务完成等待时间（秒）
     * @param bufferTimeoutSeconds 额外缓冲时间（秒），用于rebalance等时间敏感场景可设置较小值
     */
    public void shutdown(long gracefulTimeoutSeconds, long forceTimeoutSeconds, long taskTimeoutSeconds, long bufferTimeoutSeconds) {
        requestShutdown();
        
        // 等待主线程完成资源清理
        try {
            long totalTimeoutSeconds = gracefulTimeoutSeconds + forceTimeoutSeconds + taskTimeoutSeconds + bufferTimeoutSeconds;
            boolean cleanupCompleted = cleanupCompleteLatch.await(totalTimeoutSeconds, TimeUnit.SECONDS);
            
            if (cleanupCompleted) {
                log.debug("Resource cleanup completed successfully for {}-{}", topicPartition.topic(), topicPartition.partition());
            } else {
                log.warn("Timeout waiting for resource cleanup to complete for {}-{}, proceeding anyway", 
                        topicPartition.topic(), topicPartition.partition());
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for resource cleanup for {}-{}", 
                    topicPartition.topic(), topicPartition.partition());
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * 请求关闭处理器
     * 设置停止标志并中断主线程，线程池清理将在主线程结束时自动进行
     */
    public void requestShutdown() {
        log.info("Requesting shutdown for TopicPartitionProcessor {}-{}", topicPartition.topic(), topicPartition.partition());
        running = false;
        this.interrupt();
    }
    
    /**
     * 清理线程池资源
     * 该方法在主线程结束时自动调用，确保线程池资源得到正确清理
     * 
     * @param gracefulTimeoutSeconds 优雅关闭等待时间（秒）
     * @param forceTimeoutSeconds 强制关闭等待时间（秒）
     * @param taskTimeoutSeconds 任务完成等待时间（秒）
     */
    private void cleanupResources(long gracefulTimeoutSeconds, long forceTimeoutSeconds, long taskTimeoutSeconds) {
        // 如果使用了异步处理（线程池不为null），需要关闭线程池
        if (processingExecutor != null) {
            log.debug("Cleaning up processing executor for {}-{}", topicPartition.topic(), topicPartition.partition());
            
            // 关闭线程池，不再接受新任务
            processingExecutor.shutdown();
            
            try {
                // 等待现有任务完成
                if (!processingExecutor.awaitTermination(gracefulTimeoutSeconds, TimeUnit.SECONDS)) {
                    log.warn("Processing executor did not terminate gracefully for {}-{}, forcing shutdown", 
                            topicPartition.topic(), topicPartition.partition());
                    processingExecutor.shutdownNow();
                    
                    // 再等待强制关闭时间
                    if (!processingExecutor.awaitTermination(forceTimeoutSeconds, TimeUnit.SECONDS)) {
                        log.error("Processing executor did not terminate for {}-{}", 
                                topicPartition.topic(), topicPartition.partition());
                    }
                }
                
                // 等待所有待处理任务完成
                if (pendingTasks != null && !pendingTasks.isEmpty()) {
                    CompletableFuture.allOf(pendingTasks.values().toArray(new CompletableFuture[0]))
                            .get(taskTimeoutSeconds, TimeUnit.SECONDS);
                }
                         
                // 最后尝试提交剩余的偏移量
                tryCommitSequentialOffsets();
                
                log.debug("Successfully cleaned up processing executor for {}-{}", topicPartition.topic(), topicPartition.partition());
                
            } catch (InterruptedException e) {
                log.warn("Interrupted while cleaning up resources for {}-{}", 
                        topicPartition.topic(), topicPartition.partition());
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.warn("Error during resource cleanup for {}-{}: {}", 
                        topicPartition.topic(), topicPartition.partition(), e.getMessage());
            }
        } else {
            log.debug("No processing executor to clean up for {}-{}", topicPartition.topic(), topicPartition.partition());
        }
    }
}
