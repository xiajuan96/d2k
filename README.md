# D2K - Delay to Kafka

[![License](https://img.shields.io/badge/License-LGPL%203.0-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0.html)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.xiajuan96/d2k-client.svg)](https://central.sonatype.com/artifact/io.github.xiajuan96/d2k-client)
[![Java Version](https://img.shields.io/badge/Java-8%2B-blue.svg)](https://www.oracle.com/java/)

D2K（Delay to Kafka）是一个高性能的Kafka延迟消息处理SDK，提供简单易用的API来发送和消费延迟消息。支持精确的时间控制、并发处理和灵活的配置选项。

## 功能特点

- **🚀 延迟消息发送**：支持指定延迟时间（毫秒）发送消息
- **⏰ 定时消息发送**：支持指定具体时间点发送消息
- **⚙️ 灵活的配置策略**：支持按主题配置默认延迟时间
- **🔄 并发消费处理**：支持多线程并发消费延迟消息
- **📦 统一客户端API**：提供D2kClient统一客户端，简化使用
- **🎯 精确时间控制**：基于优先级队列实现精确的延迟控制
- **🔧 异步处理支持**：支持同步和异步两种消息处理模式
- **📊 流量控制**：内置队列容量管理和分区暂停机制
- **🛡️ 高可用性**：支持Kafka消费者重平衡和故障恢复

## 项目结构

D2K项目包含以下模块：

- **d2k-client**：客户端模块，提供延迟消息发送和延迟消息消费能力
- **d2k-test**：测试模块

## 安装

### Maven

在你的 `pom.xml` 文件中添加以下依赖：

```xml
<dependency>
    <groupId>io.github.xiajuan96</groupId>
    <artifactId>d2k-client</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle

在你的 `build.gradle` 文件中添加以下依赖：

```gradle
implementation 'io.github.xiajuan96:d2k-client:1.0.0'
```

## 快速开始

### 方式一：使用统一客户端（推荐）

```java
import com.d2k.D2kClient;
import com.d2k.consumer.DelayItemHandler;
import org.apache.kafka.common.serialization.StringDeserializer;

// 创建生产者配置
Map<String, Object> producerProps = new HashMap<>();
producerProps.put("bootstrap.servers", "localhost:9092");
producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

// 创建消费者配置
Map<String, Object> consumerConfigs = new HashMap<>();
consumerConfigs.put("bootstrap.servers", "localhost:9092");
consumerConfigs.put("group.id", "my-group");
consumerConfigs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
consumerConfigs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
consumerConfigs.put("auto.offset.reset", "earliest");

// 创建消息处理器
DelayItemHandler<String, String> handler = item -> {
    System.out.printf("处理延迟消息: topic=%s, key=%s, value=%s%n",
            item.getRecord().topic(), item.getRecord().key(), item.getRecord().value());
};

// 创建D2K统一客户端
D2kClient<String, String> client = new D2kClient<>(
    producerProps, consumerConfigs,
    new StringDeserializer(), new StringDeserializer(),
    Collections.singletonList("my-topic"),
    handler, 3 // 3个消费线程
);

// 启动消费者
client.startConsumer();

// 发送延迟消息（5秒后消费）
client.sendWithDelay("my-topic", "key1", "value1", 5000);

// 发送定时消息（在指定时间点消费）
long deliverAt = System.currentTimeMillis() + 10000; // 10秒后
client.sendDeliverAt("my-topic", "key2", "value2", deliverAt);

// 关闭客户端
client.close();
```

### 方式二：分别使用生产者和消费者

#### 发送延迟消息

```java
import com.d2k.producer.DelayProducer;

// 创建Kafka生产者配置
Map<String, Object> props = new HashMap<>();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

// 创建延迟消息生产者
DelayProducer<String, String> producer = new DelayProducer<>(props);

// 发送延迟消息（5秒后消费）
producer.sendWithDelay("my-topic", "key1", "value1", 5000);

// 发送定时消息（在指定时间点消费）
long deliverAt = System.currentTimeMillis() + 10000; // 10秒后
producer.sendDeliverAt("my-topic", "key2", "value2", deliverAt);

// 关闭生产者
producer.close();
```

#### 消费延迟消息

```java
import com.d2k.consumer.DelayConsumerContainer;
import com.d2k.consumer.DelayItemHandler;
import org.apache.kafka.common.serialization.StringDeserializer;

// 创建Kafka消费者配置
Map<String, Object> consumerProps = new HashMap<>();
consumerProps.put("bootstrap.servers", "localhost:9092");
consumerProps.put("group.id", "my-group");
consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
consumerProps.put("auto.offset.reset", "earliest");

// 创建消息处理器
DelayItemHandler<String, String> handler = item -> {
    System.out.printf("处理延迟消息: topic=%s, partition=%d, offset=%d, key=%s, value=%s%n",
            item.getRecord().topic(),
            item.getRecord().partition(),
            item.getRecord().offset(),
            item.getRecord().key(),
            item.getRecord().value());
};

// 创建延迟消息消费者容器
DelayConsumerContainer<String, String> container = new DelayConsumerContainer<>(
    consumerProps,
    new StringDeserializer(),
    new StringDeserializer(),
    Arrays.asList("my-topic"),
    handler,
    3 // 3个消费线程
);

// 启动消费者
container.start();

// 关闭消费者
container.close();
```

## 工作原理

D2K基于Kafka消息头机制实现延迟消息处理，核心原理如下：

### 1. 消息延迟标记
- **生产者端**：通过消息头 `d2k-deliver-at` 标记消息的预期处理时间
- **时间计算**：支持相对延迟时间（毫秒）和绝对时间戳两种方式
- **透明传输**：延迟信息不影响消息体内容，保持业务数据完整性

### 2. 消费者端处理流程
- **消息拉取**：正常从Kafka拉取消息，不影响Kafka原生性能
- **延迟检测**：解析消息头中的 `d2k-deliver-at` 时间戳
- **队列管理**：未到期消息进入优先级队列等待，到期消息立即处理
- **分区暂停**：当队列容量达到上限时，暂停对应分区的消费

### 3. 精确时间控制
- **优先级队列**：基于 `PriorityQueue` 实现，确保消息按时间顺序处理
- **多线程处理**：支持多个消费线程并发处理到期消息
- **定时检查**：后台定时器定期检查队列，处理到期消息
- **毫秒级精度**：支持毫秒级的延迟时间控制

### 4. 高可用性保障
- **消费者重平衡**：支持Kafka消费者组重平衡，确保高可用
- **故障恢复**：消费者重启后自动恢复未处理的延迟消息
- **流量控制**：内置队列容量管理，防止内存溢出

## 配置选项

### 核心配置参数

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `d2k.loop.total.ms` | Long | 1000 | 延迟消息检查间隔（毫秒） |
| `d2k.queue.capacity` | Integer | 10000 | 延迟消息队列容量 |
| `d2k.consumer.threads` | Integer | 1 | 消费者线程数量 |
| `d2k.pause.threshold` | Double | 0.8 | 队列暂停阈值（队列使用率） |

### 生产者配置

除了标准的Kafka生产者配置外，D2K支持以下扩展配置：

```java
Map<String, Object> producerProps = new HashMap<>();
// 标准Kafka配置
producerProps.put("bootstrap.servers", "localhost:9092");
producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

// D2K扩展配置
producerProps.put("d2k.loop.total.ms", 500L); // 检查间隔500ms
```

### 消费者配置

除了标准的Kafka消费者配置外，D2K支持以下扩展配置：

```java
Map<String, Object> consumerProps = new HashMap<>();
// 标准Kafka配置
consumerProps.put("bootstrap.servers", "localhost:9092");
consumerProps.put("group.id", "my-group");
consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
consumerProps.put("auto.offset.reset", "earliest");

// D2K扩展配置
consumerProps.put("d2k.loop.total.ms", 500L);     // 检查间隔500ms
consumerProps.put("d2k.queue.capacity", 5000);    // 队列容量5000
consumerProps.put("d2k.pause.threshold", 0.9);    // 暂停阈值90%
```

## 高级用法

### 异步消息处理

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

### 自定义序列化器

```java
// 使用JSON序列化器处理复杂对象
D2kClient<String, MyObject> client = new D2kClient<>(
    producerProps, consumerProps,
    new StringDeserializer(), new JsonDeserializer<>(MyObject.class),
    topics, handler, threadCount
);
```

### 批量发送延迟消息

```java
// 批量发送多个延迟消息
List<ProducerRecord<String, String>> records = Arrays.asList(
    new ProducerRecord<>("topic1", "key1", "value1"),
    new ProducerRecord<>("topic1", "key2", "value2"),
    new ProducerRecord<>("topic1", "key3", "value3")
);

long delayMs = 5000; // 5秒延迟
for (ProducerRecord<String, String> record : records) {
    client.sendWithDelay(record.topic(), record.key(), record.value(), delayMs);
}
```

## 最佳实践

### 1. 性能优化
- **合理设置线程数**：根据消息处理复杂度和系统资源调整消费者线程数
- **队列容量配置**：根据内存大小和消息量配置合适的队列容量
- **检查间隔调优**：根据延迟精度要求调整 `d2k.loop.total.ms` 参数

### 2. 可靠性保障
- **消费者组配置**：使用不同的消费者组隔离不同的业务场景
- **异常处理**：在消息处理器中添加适当的异常处理逻辑
- **监控告警**：监控队列大小、处理延迟等关键指标

### 3. 资源管理
- **及时关闭**：应用关闭时确保调用 `client.close()` 释放资源
- **连接池复用**：在同一应用中复用D2K客户端实例
- **内存监控**：监控延迟消息队列的内存使用情况

## 版本管理

本项目使用Maven Versions Plugin进行版本管理，推荐使用以下命令统一更新所有模块版本：

### 更新所有模块版本

更新根项目和所有子模块的版本，包括独立管理版本的模块：

**使用示例**：
```bash
# 第一步：更新根项目（这会同时更新d2k-test的父版本引用）
mvn versions:set -DnewVersion=1.0.2 -N

# 第二步：更新d2k-client独立模块
mvn versions:set -DnewVersion=1.0.2 -pl d2k-client

# 第三步：提交所有更改
mvn versions:commit
```

### 版本管理最佳实践

1. **开发阶段**：使用SNAPSHOT版本（如 `1.1.0-SNAPSHOT`）
2. **发布阶段**：使用正式版本（如 `1.1.0`）
3. **统一版本**：推荐使用方式3同时更新所有模块版本，保持版本一致性
4. **回滚操作**：如果需要撤销版本更改，可以使用 `mvn versions:revert`
5. **备份文件**：版本更新会自动创建备份文件（.versionsBackup），提交后自动删除

## 常见问题

### Q: 延迟消息的精度如何？
A: D2K支持毫秒级的延迟精度，实际精度取决于 `d2k.loop.total.ms` 配置和系统负载。

### Q: 如何处理大量延迟消息？
A: 可以通过增加消费者线程数、调整队列容量、使用多个消费者组等方式提高处理能力。

### Q: 消费者重启后延迟消息会丢失吗？
A: 不会。延迟消息存储在Kafka中，消费者重启后会重新拉取并处理。

### Q: 是否支持跨时区的延迟消息？
A: 支持。D2K使用UTC时间戳，不受时区影响。

## 许可证

本项目采用 [GNU Lesser General Public License v3.0 (LGPL-3.0)](https://www.gnu.org/licenses/lgpl-3.0.html) 开源许可证。

LGPL-3.0 是一个宽松的开源许可证，允许您：
- 自由使用、修改和分发本软件
- 在商业项目中使用本软件
- 将本软件作为库链接到您的应用程序中

如果您修改了本软件的源代码并分发，则必须在相同的 LGPL-3.0 许可证下提供修改后的源代码。

## 贡献

欢迎提交Issue和Pull Request来改进这个项目！

## 作者

- **xiajuan96** - *项目维护者* - [GitHub](https://github.com/xiajuan96)