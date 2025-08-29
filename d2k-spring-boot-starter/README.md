# D2K Spring Boot Starter

D2K Spring Boot Starter 为 D2K 延迟消息系统提供了 Spring Boot 自动配置和注解支持，让您能够在 Spring 应用中轻松使用延迟消息功能。

## 功能特性

- **自动配置**: 基于 Spring Boot 的自动配置机制
- **模块化配置**: Producer 和 Consumer 配置分离，支持按需加载
- **注解支持**: 提供 `@D2kListener` 注解简化消费者配置
- **模板类**: 提供 `D2kTemplate` 简化消息发送
- **配置属性**: 支持通过 `application.yml` 配置所有参数
- **生命周期管理**: 自动管理消费者容器的启动和停止
- **按需使用**: 支持只使用 Producer 或只使用 Consumer 功能

## 快速开始

### 1. 添加依赖

```xml
<dependency>
    <groupId>io.github.xiajuan96</groupId>
    <artifactId>d2k-spring-boot-starter</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 2. 启用 D2K

在 Spring Boot 主类上添加 `@EnableD2k` 注解：

```java
@SpringBootApplication
@EnableD2k
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
```

### 3. 配置参数

在 `application.yml` 中配置 D2K 参数：

```yaml
d2k:
  producer:
    bootstrap-servers: localhost:9092
    client-id: my-producer
  consumer:
    bootstrap-servers: localhost:9092
    group-id: my-consumer-group
    client-id: my-consumer
```

### 4. 发送延迟消息

使用 `D2kTemplate` 发送延迟消息（使用预配置的延迟时间）：

```java
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Component
public class MessageProducer {
    
    @Autowired
    private D2kTemplate<String, String> d2kTemplate;
    
    @Autowired
    private StringD2kTemplate stringD2kTemplate;
    
    public void sendDelayMessage() {
        // 使用 D2kTemplate 发送延迟消息（使用预配置的延迟时间）
        d2kTemplate.send("order-topic", "order-123", "Order data");
        
        // 异步发送，获取 Future 结果
        Future<RecordMetadata> future = d2kTemplate.sendAsync("payment-topic", "pay-456", "Payment data");
        
        // 同步发送，直接获取结果
        RecordMetadata result = d2kTemplate.sendSync("notification-topic", "notif-789", "Notification data");
        
        // 使用 StringD2kTemplate 发送字符串消息（无需指定 key）
        stringD2kTemplate.send("order-topic", "Simple order message");
        
        // 同步发送字符串消息，带超时控制
        RecordMetadata metadata = stringD2kTemplate.sendSync("urgent-topic", "Urgent message", 5, TimeUnit.SECONDS);
    }
}
```

### 5. 消费延迟消息

使用 `@D2kListener` 注解处理延迟消息：

```java
@Component
public class MessageConsumer {
    
    @D2kListener(topic = "my-topic", groupId = "my-group")
    public void handleMessage(ConsumerRecord<String, String> record) {
        System.out.println("收到消息: " + record.value());
    }
    
    // 简化版本，只接收消息内容
    @D2kListener(topic = "simple-topic", groupId = "simple-group")
    public void handleSimpleMessage(String message) {
        System.out.println("收到消息: " + message);
    }
}
```

## 模块化配置

D2K Spring Boot Starter 采用模块化设计，将 Producer 和 Consumer 配置分离到独立的配置类中：

- **D2kProducerAutoConfiguration**: 负责 Producer 相关 Bean 的配置
- **D2kConsumerAutoConfiguration**: 负责 Consumer 相关 Bean 的配置
- **D2kAutoConfiguration**: 主配置类，导入上述两个配置类

### 使用场景

#### 1. 只使用 Producer（发送消息）

如果您的服务只需要发送延迟消息，可以通过以下方式排除 Consumer 配置：

```java
@SpringBootApplication
@EnableD2k
@EnableAutoConfiguration(exclude = {D2kConsumerAutoConfiguration.class})
public class ProducerOnlyApplication {
    public static void main(String[] args) {
        SpringApplication.run(ProducerOnlyApplication.class, args);
    }
}
```

#### 2. 只使用 Consumer（消费消息）

如果您的服务只需要消费延迟消息，可以通过以下方式排除 Producer 配置：

```java
@SpringBootApplication
@EnableD2k
@EnableAutoConfiguration(exclude = {D2kProducerAutoConfiguration.class})
public class ConsumerOnlyApplication {
    public static void main(String[] args) {
        SpringApplication.run(ConsumerOnlyApplication.class, args);
    }
}
```

#### 3. 同时使用 Producer 和 Consumer

默认情况下，两个配置类都会被加载：

```java
@SpringBootApplication
@EnableD2k
public class FullFeaturedApplication {
    public static void main(String[] args) {
        SpringApplication.run(FullFeaturedApplication.class, args);
    }
}
```

### 配置类说明

- **D2kProducerAutoConfiguration** 提供：
  - `DelayProducer` Bean
  - `D2kTemplate` Bean

- **D2kConsumerAutoConfiguration** 提供：
  - `DelayConsumerContainer` Bean
  - `D2kConsumerManager` Bean
  - `D2kListenerAnnotationBeanPostProcessor` Bean

## 详细配置

### 默认配置说明

D2K Spring Boot Starter 提供了开箱即用的默认配置，在不主动配置的情况下，将使用以下默认值：

#### Producer 默认值
- **bootstrap-servers**: `localhost:9092`
- **key-serializer**: `org.apache.kafka.common.serialization.StringSerializer`
- **value-serializer**: `org.apache.kafka.common.serialization.StringSerializer`
- **client-id**: `d2k-producer`
- **retries**: `3`
- **batch-size**: `16384`
- **buffer-memory**: `33554432`
- **topic-delays**: `{}` (空映射，需要手动配置各 topic 的延迟时间)

#### Consumer 默认值
- **bootstrap-servers**: `localhost:9092`
- **key-deserializer**: `org.apache.kafka.common.serialization.StringDeserializer`
- **value-deserializer**: `org.apache.kafka.common.serialization.StringDeserializer`
- **group-id**: `d2k-consumer-group`
- **client-id**: `d2k-consumer`
- **enable-auto-commit**: `true`
- **session-timeout-ms**: `30000`
- **heartbeat-interval-ms**: `3000`
- **max-poll-records**: `500`
- **fetch-max-wait-ms**: `500`
- **concurrency**: `1`
- **topic-delays**: `{}` (空映射，当消息没有延迟头部时使用的默认延迟时间)

> **注意**: 默认的序列化器配置支持字符串类型的消息处理，如果需要处理其他数据类型，请根据需要修改相应的序列化器配置。

### 生产者配置

```yaml
d2k:
  producer:
    bootstrap-servers: localhost:9092          # Kafka 服务器地址
    client-id: d2k-producer                     # 客户端ID
    key-serializer: org.apache.kafka.common.serialization.StringSerializer
    value-serializer: org.apache.kafka.common.serialization.StringSerializer
    acks: all                                   # 确认模式
    retries: 3                                  # 重试次数
    batch-size: 16384                           # 批处理大小
    linger-ms: 1                                # 延迟时间
    buffer-memory: 33554432                     # 缓冲区大小
    topic-delays:                               # Topic 延迟配置（毫秒）
      order-topic: 5000                         # 订单主题延迟 5 秒
      payment-topic: 10000                      # 支付主题延迟 10 秒
      notification-topic: 3000                  # 通知主题延迟 3 秒
```

### 消费者配置

```yaml
d2k:
  consumer:
    bootstrap-servers: localhost:9092          # Kafka 服务器地址
    group-id: d2k-consumer-group               # 消费者组ID
    client-id: d2k-consumer                     # 客户端ID
    key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    enable-auto-commit: false                   # 是否自动提交偏移量
    auto-offset-reset: earliest                 # 偏移量重置策略
    session-timeout-ms: 30000                   # 会话超时时间
    heartbeat-interval-ms: 3000                 # 心跳间隔
    max-poll-records: 500                       # 最大拉取记录数
    poll-timeout-ms: 1000                       # 拉取超时时间
    concurrency: 1                              # 并发消费者数量
    topic-delays:                               # Topic 默认延迟配置（毫秒）
      order-topic: 5000                         # 订单主题默认延迟 5 秒
      payment-topic: 10000                      # 支付主题默认延迟 10 秒
      notification-topic: 3000                  # 通知主题默认延迟 3 秒
```

## 注解详解

### @EnableD2k

启用 D2K 功能的注解，需要在 Spring Boot 主类上使用。

### @D2kListener

延迟消息监听器注解，支持以下属性：

- `topic`: 监听的主题（必需）
- `groupId`: 消费者组ID（可选，默认使用配置文件中的值）
- `clientId`: 客户端ID（可选，默认使用配置文件中的值）
- `concurrency`: 并发消费者数量（可选，默认使用配置文件中的值）
- `autoStartup`: 是否自动启动（可选，默认为 true）

支持的方法签名：

```java
// 接收完整的 ConsumerRecord
@D2kListener(topic = "topic1")
public void handle(ConsumerRecord<String, String> record) { }

// 只接收消息内容
@D2kListener(topic = "topic1")
public void handle(String message) { }

// 无参数方法
@D2kListener(topic = "topic1")
public void handle() { }
```

## API 参考

### D2kTemplate

主要方法：

- `sendWithDelay(topic, value, delayMs)`: 发送延迟消息
- `sendDeliverAt(topic, value, deliverAtEpochMs)`: 发送定时消息
- `sendMessage(topic, value)`: 发送普通消息

### D2kConsumerManager

消费者容器管理器，提供以下功能：

- `startContainer(name)`: 启动指定容器
- `stopContainer(name)`: 停止指定容器
- `startAll()`: 启动所有容器
- `stopAll()`: 停止所有容器
- `isRunning(name)`: 检查容器运行状态

## 示例项目

完整的示例代码请参考 `src/test/java/com/d2k/spring/boot/autoconfigure/example/` 目录下的示例应用。

## 注意事项

1. 确保 Kafka 服务器正在运行
2. 主题需要预先创建或启用自动创建
3. 消费者组ID 应该在不同的应用实例间保持一致
4. 延迟消息的精度取决于消费者的轮询间隔
5. 建议在生产环境中适当调整批处理和并发参数

## 故障排除

### 常见问题

1. **消息没有延迟投递**
   - 检查消息头是否正确设置
   - 确认消费者正在运行
   - 查看日志中的错误信息

2. **消费者无法启动**
   - 检查 Kafka 连接配置
   - 确认主题是否存在
   - 检查消费者组权限

3. **性能问题**
   - 调整批处理大小和并发数量
   - 优化序列化器配置
   - 监控 JVM 内存使用情况

### 日志配置

启用调试日志以获取更多信息：

```yaml
logging:
  level:
    com.d2k: DEBUG
    org.apache.kafka: INFO
```