# D2K - Delay to Kafka

[![License](https://img.shields.io/badge/License-LGPL%203.0-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0.html)

D2K是一个简单的Kafka延迟消费项目，允许你发送延迟消息并在指定的时间后进行消费。

## 功能特点

- **延迟消息发送**：支持指定延迟时间（毫秒）发送消息
- **定时消息发送**：支持指定具体时间点发送消息
- **可配置的延迟策略**：支持按主题和分区配置默认延迟时间
- **并发消费**：支持多线程并发消费延迟消息
- **简单易用的API**：提供简洁的生产者和消费者API

## 项目结构

D2K项目包含以下模块：

- **d2k-producer**：延迟消息生产者模块
- **d2k-consumer**：延迟消息消费者模块
- **d2k-client**：客户端模块，整合生产者和消费者功能
- **d2k-test**：测试模块

## 安装

### Maven

```xml
<dependency>
    <groupId>io.github.xiajuan96</groupId>
    <artifactId>d2k-client</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## 快速开始

### 发送延迟消息

```java
// 创建Kafka生产者配置
Properties props = new Properties();
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

### 消费延迟消息

```java
// 创建Kafka消费者配置
Map<String, Object> configs = new HashMap<>();
configs.put("bootstrap.servers", "localhost:9092");
configs.put("group.id", "my-group");
configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
configs.put("auto.offset.reset", "earliest");

// 创建主题列表
List<String> topics = Collections.singletonList("my-topic");

// 创建延迟配置（可选，按主题和分区配置默认延迟时间）
Map<String, Map<Integer, Long>> delayConfig = new HashMap<>();
Map<Integer, Long> topicDelays = new HashMap<>();
topicDelays.put(0, 1000L); // 分区0默认延迟1秒
topicDelays.put(1, 2000L); // 分区1默认延迟2秒
delayConfig.put("my-topic", topicDelays);

// 创建消息处理器
DelayItemHandler<String, String> handler = item -> {
    ConsumerRecord<String, String> record = item.getRecord();
    System.out.printf("处理消息: topic=%s, partition=%d, offset=%d, key=%s, value=%s%n",
            record.topic(), record.partition(), record.offset(),
            record.key(), record.value());
};

// 创建并启动延迟消费容器（使用3个消费线程）
DelayConsumerContainer container = new DelayConsumerContainer(3);
container.start(configs, new StringDeserializer(), new StringDeserializer(),
        topics, delayConfig, handler);

// 应用运行中...

// 关闭消费容器
// container.stop();
```

## 工作原理

D2K通过以下方式实现延迟消费：

1. **生产者端**：在消息头部添加延迟信息
   - `d2k-delay-ms`：表示延迟多少毫秒后消费
   - `d2k-deliver-at`：表示在指定的时间戳消费

2. **消费者端**：
   - 从Kafka拉取消息
   - 解析消息头部中的延迟信息
   - 将消息放入优先级队列（按时间排序）
   - 在指定的时间点处理消息

## 配置选项

### 生产者配置

生产者使用标准的Kafka生产者配置，无需额外配置。

### 消费者配置

除了标准的Kafka消费者配置外，还支持以下自定义配置：

- `d2k.loop.total.ms`：消费循环的总时间（毫秒），默认为200ms

## 许可证

本项目采用 [GNU Lesser General Public License v3.0 (LGPL-3.0)](https://www.gnu.org/licenses/lgpl-3.0.html) 开源许可证。

LGPL-3.0 是一个宽松的开源许可证，允许您：
- 自由使用、修改和分发本软件
- 在商业项目中使用本软件
- 将本软件作为库链接到您的应用程序中

如果您修改了本软件的源代码并分发，则必须在相同的 LGPL-3.0 许可证下提供修改后的源代码。