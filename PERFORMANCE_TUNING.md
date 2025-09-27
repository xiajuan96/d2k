# D2K 性能调优指南

本指南基于 D2K 延迟消息框架的实际实现，提供系统性的性能优化策略和最佳实践。

## 目录

- [核心性能机制](#核心性能机制)
- [消费者性能调优](#消费者性能调优)
- [生产者性能调优](#生产者性能调优)
- [内存管理优化](#内存管理优化)
- [并发处理优化](#并发处理优化)
- [Kafka 集成优化](#kafka-集成优化)
- [性能监控与诊断](#性能监控与诊断)
- [最佳实践](#最佳实践)

## 核心性能机制

### 延迟调度算法

D2K 基于 `PriorityBlockingQueue` 实现高效的延迟消息调度：

```java
// 核心调度机制
private final PriorityBlockingQueue<DelayItem<K, V>> delayQueue = 
    new PriorityBlockingQueue<>(queueCapacity, 
        Comparator.comparing(DelayItem::getDeliverAt));
```

**性能特点**：
- **时间复杂度**：插入 O(log n)，取出 O(log n)
- **内存效率**：基于堆结构，空间复杂度 O(n)
- **线程安全**：无锁化设计，高并发性能

### 消息处理流程

```
[Kafka Poll] → [延迟检查] → [队列入队] → [定时出队] → [业务处理]
     ↓              ↓           ↓           ↓           ↓
  批量拉取      毫秒级精度    优先队列    异步处理    手动提交
```

## 消费者性能调优

### 1. 核心配置参数

#### D2K 专有配置

```java
// 循环检查间隔（毫秒）
consumerConfigs.put("d2k.loop.total.ms", 100L); // 默认 200ms

// 延迟队列容量
consumerConfigs.put("d2k.queue.capacity", 5000); // 默认 1000
```

**调优建议**：
- **高吞吐场景**：`loop.total.ms` 设置为 50-100ms
- **低延迟场景**：`loop.total.ms` 设置为 10-50ms
- **大内存场景**：`queue.capacity` 可设置为 10000+

#### Kafka 原生配置优化

```java
// 批量拉取优化
consumerConfigs.put("max.poll.records", 1000);        // 单次拉取记录数
consumerConfigs.put("fetch.min.bytes", 50000);        // 最小拉取字节数
consumerConfigs.put("fetch.max.wait.ms", 100);        // 最大等待时间

// 网络优化
consumerConfigs.put("receive.buffer.bytes", 262144);  // 接收缓冲区
consumerConfigs.put("send.buffer.bytes", 131072);     // 发送缓冲区

// 会话管理
consumerConfigs.put("session.timeout.ms", 30000);     // 会话超时
consumerConfigs.put("heartbeat.interval.ms", 3000);   // 心跳间隔
```

### 2. 并发消费者配置

```java
// 创建多个消费者实例
int concurrency = Runtime.getRuntime().availableProcessors(); // CPU 核数
DelayConsumerContainer<String, String> container = new DelayConsumerContainer<>(
    concurrency,
    consumerConfigs,
    topicList,
    handler
);
```

**并发数选择策略**：
- **CPU 密集型**：并发数 = CPU 核数
- **IO 密集型**：并发数 = CPU 核数 × 2
- **混合型**：并发数 = CPU 核数 × 1.5

### 3. 异步处理优化

```java
// 异步处理配置
AsyncProcessingConfig asyncConfig = new AsyncProcessingConfig(
    true,                    // 启用异步处理
    concurrency * 2,         // 核心线程数
    concurrency * 4,         // 最大线程数
    60000L,                  // 线程空闲时间（毫秒）
    1000,                    // 队列容量
    "CallerRuns"             // 拒绝策略
);
```

**线程池调优**：
- **核心线程数**：建议为消费者并发数的 2 倍
- **最大线程数**：建议为消费者并发数的 4 倍
- **队列容量**：根据内存情况设置，建议 500-2000
- **拒绝策略**：推荐 `CallerRuns`，避免消息丢失

## 生产者性能调优

### 1. 批量发送优化

```java
// 批量发送配置
producerConfigs.put("batch.size", 32768);           // 批次大小（字节）
producerConfigs.put("linger.ms", 10);               // 批次等待时间
producerConfigs.put("buffer.memory", 67108864);     // 缓冲区大小

// 压缩配置
producerConfigs.put("compression.type", "lz4");     // 压缩算法
```

### 2. 异步发送模式

```java
// 异步发送延迟消息（带回调）
Future<RecordMetadata> future = producer.sendWithDelay(
    "my-topic", "key", "value", 5000L, new DelayCallback() {
        @Override
        public void onSuccess(RecordMetadata metadata) {
            System.out.println("消息发送成功: " + metadata);
        }
        
        @Override
        public void onFailure(Exception exception) {
            System.err.println("消息发送失败: " + exception.getMessage());
        }
    }
);

// 批量发送
List<Future<RecordMetadata>> futures = new ArrayList<>();
for (Message msg : messages) {
    futures.add(producer.sendWithDelay(
        msg.getTopic(), msg.getKey(), msg.getValue(), msg.getDelay()
    ));
}

// 等待所有发送完成
for (Future<RecordMetadata> f : futures) {
    f.get(); // 阻塞等待结果
}
```

## 内存管理优化

### 1. JVM 参数调优

```bash
# 堆内存设置
-Xms4g -Xmx4g

# 垃圾回收器选择
-XX:+UseG1GC
-XX:MaxGCPauseMillis=200
-XX:G1HeapRegionSize=16m

# 内存分析
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/path/to/dumps/
```

### 2. 延迟队列容量规划

```java
// 容量计算公式
int queueCapacity = (int) (
    (maxDelayTimeMs / loopTotalMs) * maxThroughputPerSecond / 1000
);

// 示例：最大延迟1小时，循环间隔100ms，吞吐量1000条/秒
int capacity = (int) ((3600000 / 100) * 1000 / 1000); // = 36000
```

### 3. 内存监控指标

```java
// 队列使用率监控
double queueUsageRatio = (double) delayQueue.size() / queueCapacity;
if (queueUsageRatio > 0.8) {
    logger.warn("延迟队列使用率过高: {}%", queueUsageRatio * 100);
}

// 堆内存监控
MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
double heapUsageRatio = (double) heapUsage.getUsed() / heapUsage.getMax();
```

## 并发处理优化

### 1. 消费者线程模型

```
[主线程] → [消费者线程池] → [业务处理线程池]
    ↓           ↓                ↓
  容器管理    Kafka拉取        异步处理
```

### 2. 线程安全保障

```java
// DelayConsumerRunnable 中的线程安全设计
public class DelayConsumerRunnable<K, V> implements Runnable {
    // 线程安全的优先队列
    private final PriorityBlockingQueue<DelayItem<K, V>> delayQueue;
    
    // 每个线程独立的 Kafka Consumer
    private final KafkaConsumer<K, V> consumer;
    
    // 原子操作的状态管理
    private final AtomicBoolean running = new AtomicBoolean(false);
}
```

### 3. 背压处理机制

```java
// 队列满时的处理策略
if (delayQueue.size() >= queueCapacity) {
    // 策略1：阻塞等待
    delayQueue.put(delayItem);
    
    // 策略2：丢弃最旧消息
    delayQueue.poll();
    delayQueue.offer(delayItem);
    
    // 策略3：暂停消费
    consumer.pause(consumer.assignment());
}
```

## Kafka 集成优化

### 1. 分区策略

**注意**：当前版本暂不提供自定义分区器实现，建议使用 Kafka 默认分区策略或自行实现。

### 2. 偏移量管理

```java
// D2K 强制禁用自动提交，使用手动提交
consumerConfigs.put("enable.auto.commit", false);

// 在消息处理完成后手动提交
consumer.commitSync(Collections.singletonMap(
    new TopicPartition(record.topic(), record.partition()),
    new OffsetAndMetadata(record.offset() + 1)
));
```

### 3. 连接池优化

```java
// 生产者连接池配置
producerConfigs.put("connections.max.idle.ms", 300000);  // 连接空闲时间
producerConfigs.put("reconnect.backoff.ms", 1000);       // 重连退避时间
producerConfigs.put("retry.backoff.ms", 1000);           // 重试退避时间
```

## 性能监控与诊断

### 1. 关键性能指标

**注意**：当前版本暂不提供内置性能监控指标，建议使用标准的 JVM 监控工具（如 JConsole、VisualVM）和 Kafka 客户端自带的监控指标。

### 2. 性能瓶颈诊断

```java
// CPU 使用率检查
OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
double cpuUsage = osBean.getProcessCpuLoad();

// GC 压力检查
List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
for (GarbageCollectorMXBean gcBean : gcBeans) {
    long gcTime = gcBean.getCollectionTime();
    long gcCount = gcBean.getCollectionCount();
}

// 网络延迟检查
long networkLatency = measureKafkaLatency();
```

### 3. 性能测试工具

```bash
# Kafka 性能测试
kafka-producer-perf-test.sh \
  --topic d2k-test \
  --num-records 100000 \
  --record-size 1024 \
  --throughput 10000 \
  --producer-props bootstrap.servers=localhost:9092

# D2K 延迟精度测试
# 注意：DelayAccuracyTest 测试工具暂未提供，建议自行编写测试代码
```

## 最佳实践

### 1. 配置模板

#### 高吞吐量场景

```java
// 生产者配置
Map<String, Object> producerConfigs = new HashMap<>();
producerConfigs.put("bootstrap.servers", "localhost:9092");
producerConfigs.put("batch.size", 65536);
producerConfigs.put("linger.ms", 20);
producerConfigs.put("compression.type", "lz4");
producerConfigs.put("buffer.memory", 134217728);

// 消费者配置
Map<String, Object> consumerConfigs = new HashMap<>();
consumerConfigs.put("bootstrap.servers", "localhost:9092");
consumerConfigs.put("max.poll.records", 2000);
consumerConfigs.put("fetch.min.bytes", 100000);
consumerConfigs.put("d2k.loop.total.ms", 50L);
consumerConfigs.put("d2k.queue.capacity", 10000);
```

#### 低延迟场景

```java
// 生产者配置
producerConfigs.put("batch.size", 1024);
producerConfigs.put("linger.ms", 1);
producerConfigs.put("compression.type", "none");

// 消费者配置
consumerConfigs.put("max.poll.records", 100);
consumerConfigs.put("fetch.min.bytes", 1);
consumerConfigs.put("d2k.loop.total.ms", 10L);
consumerConfigs.put("d2k.queue.capacity", 1000);
```

### 2. 容量规划

**注意**：当前版本暂不提供容量规划计算器，建议根据实际业务场景和测试结果进行容量评估。

基本计算公式：
- 队列容量 = (最大延迟时间 / 循环检查间隔) × (每秒吞吐量 / 1000) × 安全系数
- 安全系数建议设置为 1.5-2.0



### 4. 故障恢复

```java
// 优雅关闭处理
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    logger.info("开始优雅关闭 D2K 消费者...");
    
    // 1. 停止接收新消息
    container.stop();
    
    // 2. 等待队列中的消息处理完成
    while (!delayQueue.isEmpty()) {
        Thread.sleep(100);
    }
    
    // 3. 关闭线程池
    executorService.shutdown();
    
    // 4. 提交最终偏移量
    consumer.commitSync();
    
    logger.info("D2K 消费者已优雅关闭");
}));
```



## 相关文档

### 使用指南
- [README](./README.md) - 项目概览和快速开始
- [高级用法](./ADVANCED_USAGE.md) - 详细配置和最佳实践
- [配置分离指南](./CONFIG_SEPARATION_GUIDE.md) - D2K配置机制详解

### 开发指南
- [开发指南](./DEVELOPER_GUIDE.md) - 完整API文档和开发规范

### 运维指南
- **监控功能说明**：当前版本暂不提供监控支持功能，监控能力已规划为未来版本的开发计划

### 问题解决
- [常见问题](./FAQ.md) - 性能问题排查和解决方案

---

**注意**：本指南基于 D2K 1.0.x 版本编写，具体的性能表现可能因环境而异，建议在实际环境中进行充分测试。