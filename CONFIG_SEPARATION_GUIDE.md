# D2K消费者配置分离指南

本文档说明了DelayConsumerRunnable中配置参数的分离和使用方式。

## 配置分离概述

DelayConsumerRunnable现在将配置参数分为两类：
1. **Kafka原生配置** - 传递给KafkaConsumer的标准Kafka配置
2. **D2K专有配置** - 通过D2kConsumerConfig类管理的D2K框架特有配置

## 配置分类

### Kafka原生配置
所有不以"d2k."开头的配置项都被视为Kafka原生配置，例如：
- `bootstrap.servers` - Kafka集群地址
- `group.id` - 消费者组ID
- `client.id` - 客户端ID
- `auto.offset.reset` - 偏移量重置策略
- `session.timeout.ms` - 会话超时时间
- `heartbeat.interval.ms` - 心跳间隔
- `max.poll.records` - 单次拉取最大记录数
- `max.poll.interval.ms` - 拉取间隔
- 其他标准Kafka消费者配置

### D2K专有配置
所有以"d2k."开头的配置项都被视为D2K专有配置，通过D2kConsumerConfig类管理：
- `d2k.loop.total.ms` - 循环总时间（默认：200ms）
- `d2k.queue.capacity` - 队列容量阈值（默认：1000）

## D2kConsumerConfig类

D2kConsumerConfig是一个专门的配置类，提供：
- **类型安全**: 配置值的类型检查和转换
- **参数验证**: 配置值的有效性验证
- **默认值管理**: 统一的默认值定义
- **配置提取**: 从混合配置中提取D2K专有配置

## 队列暂停逻辑

队列的暂停和恢复逻辑完全基于 `d2k.queue.capacity` 的值：

- **暂停条件**: 当队列使用率超过100%（即队列大小超过队列容量阈值）时，会暂停对应分区的消费
- **恢复条件**: 当队列使用率降到100%以下时，会恢复对应分区的消费
- **使用率计算**: `使用率 = 队列当前大小 / d2k.queue.capacity`

这种设计确保了队列不会无限增长，当队列达到容量阈值时自动进行流量控制。


## 使用示例

### 基本用法
```java
Map<String, Object> configs = new HashMap<>();

// Kafka原生配置
configs.put("bootstrap.servers", "localhost:9092");
configs.put("group.id", "my-consumer-group");
configs.put("auto.offset.reset", "earliest");

// D2K专有配置
configs.put("d2k.loop.total.ms", 300L);
configs.put("d2k.queue.capacity", 2000);

// 创建DelayConsumerRunnable
DelayConsumerRunnable<String, String> runnable = new DelayConsumerRunnable<>(
    configs,
    new StringDeserializer(),
    new StringDeserializer(),
    Arrays.asList("my-topic"),
    delayItemHandler
);
```

### 获取配置信息
```java
// 获取Kafka原生配置
Map<String, Object> kafkaConfigs = runnable.getKafkaConfigs();
System.out.println("Kafka configs: " + kafkaConfigs);

// 获取D2K专有配置（Map形式，向后兼容）
Map<String, Object> d2kConfigs = runnable.getD2kConfigs();
System.out.println("D2K configs: " + d2kConfigs);

// 获取D2K配置对象（推荐方式）
D2kConsumerConfig d2kConfig = runnable.getD2kConfig();
long loopTotalMs = d2kConfig.getLoopTotalMs();
int queueCapacity = d2kConfig.getQueueCapacity();
```

### 直接使用D2kConsumerConfig
```java
// 创建D2K配置对象
Map<String, Object> d2kConfigs = new HashMap<>();
d2kConfigs.put(D2kConsumerConfig.LOOP_TOTAL_MS_CONFIG, 250L);
d2kConfigs.put(D2kConsumerConfig.QUEUE_CAPACITY_CONFIG, 1500);

D2kConsumerConfig config = new D2kConsumerConfig(d2kConfigs);

// 类型安全的配置访问
long loopTime = config.getLoopTotalMs();  // 250L
int capacity = config.getQueueCapacity();  // 1500

// 配置验证（如果值无效会抛出异常）
// config会自动验证loopTotalMs > 0 和 queueCapacity > 0
```

## 向后兼容性

现有代码无需修改，配置拆分在内部自动进行：
- 所有以 `d2k.` 开头的配置项自动归类为D2K专有配置
- 其他所有配置项自动归类为Kafka原生配置
- 原有的构造函数和API保持不变

## 优势

1. **配置清晰分离**：Kafka和D2K配置明确区分，避免混淆
2. **类型安全**：每种配置都有明确的用途和作用域
3. **易于维护**：配置管理更加规范和可维护
4. **向后兼容**：现有代码无需修改即可使用新的配置拆分功能