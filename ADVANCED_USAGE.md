# D2K 高级使用指南

本文档提供 D2K (Delay to Kafka) 的高级使用方式、详细配置说明和最佳实践。

## 目录

- [自定义消息处理器](#自定义消息处理器)
- [配置延迟策略](#配置延迟策略)
- [异步处理配置](#异步处理配置)
- [配置分离示例](#配置分离示例)
- [最佳实践](#最佳实践)
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

### 4. 延迟配置策略

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

### 5. 资源管理和优雅关闭

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

## 相关文档

### 开发相关
- [开发指南](./DEVELOPER_GUIDE.md) - 完整API文档和开发规范
- [配置分离指南](./CONFIG_SEPARATION_GUIDE.md) - D2K配置机制深入解析

### 运维相关
- [性能调优指南](./PERFORMANCE_TUNING.md) - 系统性能优化策略
- **监控功能说明**：当前版本暂不提供监控支持功能，监控能力已规划为未来版本的开发计划

### 问题解决
- [常见问题](./FAQ.md) - 常见问题排查和解决方案
- [README](./README.md) - 项目概览和快速开始

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