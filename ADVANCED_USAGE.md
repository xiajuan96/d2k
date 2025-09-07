# D2K 高级使用指南

本文档提供 D2K (Delay to Kafka) 的高级使用方式、详细配置说明和最佳实践。

## 目录

- [自定义消息处理器](#自定义消息处理器)
- [配置延迟策略](#配置延迟策略)
- [异步处理配置](#异步处理配置)
- [配置分离示例](#配置分离示例)
- [异步消息处理](#异步消息处理)
- [自定义序列化器](#自定义序列化器)
- [批量发送延迟消息](#批量发送延迟消息)
- [最佳实践](#最佳实践)
- [常见问题解答](#常见问题解答)
- [API 接口文档](#api-接口文档)

## 自定义消息处理器

实现 `DelayItemHandler` 接口来自定义消息处理逻辑：

```java
public class CustomDelayItemHandler implements DelayItemHandler<String, String> {
    private static final Logger log = LoggerFactory.getLogger(CustomDelayItemHandler.class);
    
    @Override
    public void process(DelayItem<String, String> item) {
        ConsumerRecord<String, String> record = item.getRecord();
        
        try {
            // 业务处理
            processBusinessLogic(record.key(), record.value());
            
            // 记录处理日志
            log.info("Processed delayed message: topic={}, partition={}, offset={}, key={}", 
                    record.topic(), record.partition(), record.offset(), record.key());
        } catch (Exception e) {
            log.error("Failed to process delayed message: {}", record.value(), e);
            // 可以实现重试逻辑或错误处理
        }
    }
    
    private void processBusinessLogic(String key, String value) {
        // 实现具体的业务逻辑
        // 例如：数据库操作、外部API调用等
    }
}
```

## 配置延迟策略

使用 `DelayConfigBuilder` 创建复杂的延迟配置：

```java
// 使用DelayConfigBuilder创建延迟配置
DelayConfig delayConfig = new DelayConfigBuilder()
    .withTopicDelay("order-topic", 30000L)        // 订单主题延迟30秒
    .withTopicDelay("notification-topic", 5000L)  // 通知主题延迟5秒
    .withTopicPartitionDelay("payment-topic", 0, 10000L)  // 支付主题分区0延迟10秒
    .withTopicPartitionDelay("payment-topic", 1, 15000L)  // 支付主题分区1延迟15秒
    .build();

// 使用配置创建生产者
ConfigurableDelayProducer<String, String> producer = 
    new ConfigurableDelayProducer<>(kafkaProps, delayConfig);

// 发送消息时自动应用延迟配置
producer.send("order-topic", "order-123", orderData);     // 自动延迟30秒
producer.send("payment-topic", 0, "pay-456", paymentData); // 自动延迟10秒
```

## 异步处理配置

异步处理模式适用于需要高吞吐量的场景，通过线程池并行处理延迟消息，避免单个消息的处理时间影响整体性能。

### 配置参数说明

通过`AsyncProcessingConfig`类配置异步处理参数：

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `enabled` | Boolean | false | 是否启用异步处理 |
| `corePoolSize` | Integer | 2 | 核心线程数 |
| `maximumPoolSize` | Integer | 4 | 最大线程数 |
| `keepAliveTime` | Long | 60 | 线程空闲时间（秒） |
| `queueCapacity` | Integer | 100 | 任务队列长度 |
| `rejectedExecutionPolicy` | Enum | CALLER_RUNS | 拒绝策略 |

### 基本使用示例

```java
// 创建异步处理配置
AsyncProcessingConfig asyncConfig = AsyncProcessingConfig.createAsyncConfig(
    2,   // 核心线程数
    4,   // 最大线程数
    100  // 队列长度
);

// 创建延迟消息消费者容器（异步处理）
DelayConsumerContainer<String, String> container = new DelayConsumerContainer<>(
    3, // 3个消费线程
    consumerProps,
    Arrays.asList("my-topic"),
    handler,
    asyncConfig  // 异步处理配置
);

container.start();
```

### 高级配置示例

```java
// 创建自定义异步处理配置
AsyncProcessingConfig asyncConfig = new AsyncProcessingConfig();
asyncConfig.setEnabled(true);
asyncConfig.setCorePoolSize(4);
asyncConfig.setMaximumPoolSize(8);
asyncConfig.setQueueCapacity(200);
asyncConfig.setRejectedExecutionPolicy(AsyncProcessingConfig.RejectedExecutionPolicy.CALLER_RUNS);

// 或使用工厂方法
AsyncProcessingConfig asyncConfig2 = AsyncProcessingConfig.createAsyncConfig(4, 8, 200);

// 应用到消费者容器
DelayConsumerContainer<String, String> container = new DelayConsumerContainer<>(
    3, consumerProps, topics, handler, asyncConfig
);
```

## 配置分离示例

D2K 支持 Kafka 原生配置与 D2K 专有配置的分离管理：

```java
// 混合配置Map
Map<String, Object> allConfigs = new HashMap<>();

// Kafka原生配置
allConfigs.put("bootstrap.servers", "localhost:9092");
allConfigs.put("group.id", "my-group");
allConfigs.put("auto.offset.reset", "earliest");

// D2K专有配置
allConfigs.put("d2k.loop.total.ms", 300L);
allConfigs.put("d2k.queue.capacity", 2000);

// DelayConsumerRunnable会自动分离配置
DelayConsumerRunnable<String, String> runnable = new DelayConsumerRunnable<>(
    allConfigs, topics, handler, asyncConfig
);
```

## 异步消息处理

实现异步消息处理来提高性能：

```java
// 创建支持异步处理的消息处理器
DelayItemHandler<String, String> asyncHandler = item -> {
    // 异步处理消息
    CompletableFuture.runAsync(() -> {
        try {
            // 模拟耗时操作
            Thread.sleep(100);
            System.out.println("异步处理完成: " + item.getRecord().value());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    });
};
```

## 自定义序列化器

使用自定义序列化器处理复杂对象：

```java
// 使用JSON序列化器处理复杂对象
DelayConsumerContainer<String, MyObject> container = new DelayConsumerContainer<>(
    threadCount,
    consumerProps,
    topics,
    handler
);
```

## 批量发送延迟消息

批量发送多个延迟消息：

```java
// 批量发送多个延迟消息
DelayProducer<String, String> producer = new DelayProducer<>(producerProps);

List<ProducerRecord<String, String>> records = Arrays.asList(
    new ProducerRecord<>("topic1", "key1", "value1"),
    new ProducerRecord<>("topic1", "key2", "value2"),
    new ProducerRecord<>("topic1", "key3", "value3")
);

long delayMs = 5000; // 5秒延迟
for (ProducerRecord<String, String> record : records) {
    producer.sendWithDelay(record.topic(), record.key(), record.value(), delayMs);
}

producer.close();
```

## 最佳实践

### 1. 合理设置延迟时间

- 避免设置过短的延迟时间（< 1秒），可能影响性能
- 考虑业务场景的实际需求，避免不必要的长延迟
- 使用定时消息而非延迟消息处理固定时间点的任务
- 对于大量相同延迟时间的消息，考虑使用ConfigurableDelayProducer

### 2. 配置分离最佳实践

```java
// 推荐的配置方式
Map<String, Object> configs = new HashMap<>();

// Kafka原生配置
configs.put("bootstrap.servers", "localhost:9092");
configs.put("group.id", "delay-consumer-group");
configs.put("auto.offset.reset", "earliest");
configs.put("max.poll.records", "100"); // 控制批次大小
configs.put("session.timeout.ms", "30000");
configs.put("heartbeat.interval.ms", "3000");
configs.put("max.poll.interval.ms", "300000");

// D2K专有配置
configs.put("d2k.loop.total.ms", 200L);    // 根据业务调整轮询频率
configs.put("d2k.queue.capacity", 1000);   // 根据内存和吞吐量调整
```

### 3. 异步处理配置建议

```java
// 高吞吐量场景
AsyncProcessingConfig highThroughputConfig = AsyncProcessingConfig.createAsyncConfig(
    Runtime.getRuntime().availableProcessors(),     // 核心线程数
    Runtime.getRuntime().availableProcessors() * 2, // 最大线程数
    500  // 队列长度
);

// 低延迟场景
AsyncProcessingConfig lowLatencyConfig = AsyncProcessingConfig.createSyncConfig();

// 平衡场景
AsyncProcessingConfig balancedConfig = AsyncProcessingConfig.createAsyncConfig(4, 8, 200);
```

### 4. 错误处理和重试机制

```java
public class RobustDelayItemHandler implements DelayItemHandler<String, String> {
    private static final Logger log = LoggerFactory.getLogger(RobustDelayItemHandler.class);
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 1000;
    
    @Override
    public void process(DelayItem<String, String> item) {
        int retries = 0;
        Exception lastException = null;
        
        while (retries <= MAX_RETRIES) {
            try {
                processMessage(item.getRecord());
                return; // 成功处理
            } catch (Exception e) {
                lastException = e;
                retries++;
                
                if (retries <= MAX_RETRIES) {
                    log.warn("Processing failed, retry {}/{}: {}", 
                            retries, MAX_RETRIES, e.getMessage());
                    try {
                        Thread.sleep(RETRY_DELAY_MS * retries); // 指数退避
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
        
        // 所有重试都失败
        handleFailedMessage(item, lastException);
    }
    
    private void processMessage(ConsumerRecord<String, String> record) {
        // 实际业务处理逻辑
    }
    
    private void handleFailedMessage(DelayItem<String, String> item, Exception e) {
        // 记录到死信队列或错误日志
        log.error("Message processing failed after {} retries: topic={}, partition={}, offset={}", 
                 MAX_RETRIES, item.getRecord().topic(), 
                 item.getRecord().partition(), item.getRecord().offset(), e);
    }
}
```

### 5. 延迟配置策略

```java
// 按业务场景配置延迟策略
DelayConfig businessDelayConfig = new DelayConfigBuilder()
    // 订单相关 - 较长延迟
    .withTopicDelay("order-created", 30000L)      // 30秒后处理订单创建
    .withTopicDelay("order-timeout", 1800000L)    // 30分钟订单超时检查
    // 通知延迟配置
    .withTopicDelay("notification-sms", 5000L)    // 5秒后发送短信
    .withTopicDelay("notification-email", 10000L) // 10秒后发送邮件
    // 支付检查延迟配置（分区级别）
    .withTopicPartitionDelay("payment-check", 0, 60000L)  // 分区0: 1分钟
    .withTopicPartitionDelay("payment-check", 1, 120000L) // 分区1: 2分钟
    .build();
```

### 6. 监控和指标

```java
// 自定义处理器，添加监控指标
public class MonitoredDelayItemHandler implements DelayItemHandler<String, String> {
    private final DelayItemHandler<String, String> delegate;
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    
    @Override
    public void process(DelayItem<String, String> item) {
        long startTime = System.currentTimeMillis();
        try {
            delegate.process(item);
            processedCount.incrementAndGet();
        } catch (Exception e) {
            errorCount.incrementAndGet();
            throw e;
        } finally {
            long processingTime = System.currentTimeMillis() - startTime;
            // 记录处理时间指标
            recordProcessingTime(processingTime);
        }
    }
    
    public long getProcessedCount() { return processedCount.get(); }
    public long getErrorCount() { return errorCount.get(); }
    
    private void recordProcessingTime(long processingTime) {
        // 实现指标记录逻辑
    }
}
```

### 7. 资源管理和优雅关闭

```java
public class DelayMessageService {
    private DelayConsumerContainer<String, String> container;
    private DelayProducer<String, String> producer;
    
    public void start() {
        // 启动服务
        container.start();
    }
    
    public void shutdown() {
        try {
            // 优雅关闭消费者
            if (container != null) {
                container.stop();
            }
            
            // 关闭生产者
            if (producer != null) {
                producer.close();
            }
        } catch (Exception e) {
            log.error("Error during shutdown", e);
        }
    }
}
```

## 常见问题解答

详细的常见问题解答请参考：[FAQ.md](./FAQ.md)

该文档包含以下主要内容：
- **性能优化**：消息延迟、内存使用等性能相关问题
- **可靠性保障**：消息不丢失、异常处理等可靠性配置
- **配置管理**：D2K专有配置和Kafka原生配置的正确使用
- **异步处理**：同步和异步处理模式的选择建议
- **监控运维**：应用状态监控和告警配置
- **版本升级**：安全升级和回滚策略

## API 接口文档

### DelayProducer

延迟消息生产者，用于发送延迟消息。

#### 构造方法

```java
// 使用Properties配置创建
DelayProducer(Properties props)

// 使用Map配置创建
DelayProducer(Map<String, Object> configs)

// 使用现有Producer创建（测试用）
DelayProducer(Producer<K, V> producer)
```

#### 主要方法

```java
// 发送延迟消息（指定延迟时间）
Future<RecordMetadata> sendWithDelay(String topic, K key, V value, long delayMs)

// 发送定时消息（指定投递时间）
Future<RecordMetadata> sendDeliverAt(String topic, K key, V value, long deliverAt)

// 关闭生产者
void close()
```

### ConfigurableDelayProducer

可配置的延迟消息生产者，支持按主题和分区配置默认延迟时间。

#### 构造方法

```java
// 使用配置创建
ConfigurableDelayProducer(Map<String, Object> configs, DelayConfig delayConfig)

// 使用现有Producer创建（测试用）
ConfigurableDelayProducer(Producer<K, V> producer, DelayConfig delayConfig)
```

#### 主要方法

```java
// 发送消息（使用配置的默认延迟时间）
Future<RecordMetadata> send(String topic, K key, V value)
Future<RecordMetadata> send(String topic, int partition, K key, V value)

// 发送消息到指定分区
Future<RecordMetadata> send(ProducerRecord<K, V> record)

// 关闭生产者
void close()
```

### DelayConsumerContainer

延迟消息消费者容器，管理多个消费者线程。

#### 构造方法

```java
// 基本构造方法（同步处理）
DelayConsumerContainer(int concurrency, 
                      Map<String, Object> configs,
                      Collection<String> topics,
                      DelayItemHandler<K, V> handler)

// 完整构造方法（支持异步处理）
DelayConsumerContainer(int concurrency,
                      Map<String, Object> configs,
                      Collection<String> topics,
                      DelayItemHandler<K, V> handler,
                      AsyncProcessingConfig asyncProcessingConfig)
```

#### 主要方法

```java
// 启动消费者
void start()

// 停止消费者
void stop()
```

### DelayConfigBuilder

延迟配置构建器，用于创建复杂的延迟配置。

#### 主要方法

```java
// 设置主题级别的默认延迟时间
DelayConfigBuilder withTopicDelay(String topic, long delayMs)

// 设置主题分区级别的默认延迟时间
DelayConfigBuilder withTopicPartitionDelay(String topic, int partition, long delayMs)

// 构建配置对象
DelayConfig build()
```

### AsyncProcessingConfig

异步处理配置类，用于配置异步消息处理。

#### 工厂方法

```java
// 创建同步处理配置
static AsyncProcessingConfig createSyncConfig()

// 创建异步处理配置
static AsyncProcessingConfig createAsyncConfig(int corePoolSize, int maximumPoolSize, int queueCapacity)
```

#### 主要方法

```java
// 设置是否启用异步处理
void setEnabled(boolean enabled)

// 设置核心线程数
void setCorePoolSize(int corePoolSize)

// 设置最大线程数
void setMaximumPoolSize(int maximumPoolSize)

// 设置队列容量
void setQueueCapacity(int queueCapacity)

// 设置拒绝策略
void setRejectedExecutionPolicy(RejectedExecutionPolicy policy)
```