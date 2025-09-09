# D2K 开发者指南

本文档为 D2K (Delay to Kafka) 项目的开发者提供详细的技术信息，包括版本管理、环境配置、API使用说明和开发规范等。

## 目录

- [系统要求](#系统要求)
- [环境配置](#环境配置)
- [贡献流程](#贡献流程)
- [开发规范](#开发规范)
- [API 接口文档](#api-接口文档)
- [技术实现细节](#技术实现细节)
- [配置选项详解](#配置选项详解)
- [版本管理](#版本管理)
- [许可证信息](#许可证信息)

## 系统要求

### 基础环境

- **Java版本**：JDK 8 或更高版本
- **Kafka版本**：兼容 Apache Kafka 2.0+ 
- **依赖管理**：Maven 3.6+ 或 Gradle 6.0+

### 开发环境推荐

- **IDE**：IntelliJ IDEA 或 Eclipse
- **构建工具**：Maven（推荐）
- **版本控制**：Git
- **测试框架**：JUnit 4.13+

## 环境配置

### 源码获取

#### 克隆项目

```bash
git clone https://github.com/xiajuan96/d2k.git
cd d2k
```

#### 项目结构

```
d2k/
├── d2k-client/          # 核心客户端模块
├── d2k-test/            # 测试模块
├── pom.xml              # 父级 Maven 配置
├── README.md            # 项目说明
├── DEVELOPER_GUIDE.md   # 开发者指南
└── ADVANCED_USAGE.md    # 高级使用指南
```

### 本地开发环境配置

#### 构建项目

```bash
# 编译项目
mvn clean compile

# 运行测试
mvn test

# 打包项目
mvn clean package
```

#### IDE 配置

**IntelliJ IDEA**：
1. 打开 IntelliJ IDEA
2. 选择 "Open" 并导航到项目根目录
3. 选择 `pom.xml` 文件并选择 "Open as Project"
4. 等待 Maven 依赖下载完成

**Eclipse**：
1. 选择 "File" > "Import" > "Existing Maven Projects"
2. 浏览到项目根目录
3. 选择项目并点击 "Finish"

### 开发环境验证

运行以下命令验证环境配置是否正确：

```bash
# 验证编译
mvn clean compile

# 运行单元测试
mvn test -Dtest=DelayProducerTest

# 运行集成测试
mvn test -Dtest=DelayConsumerIntegrationTest
```

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

### 版本发布流程

1. **准备发布**
   ```bash
   # 确保所有测试通过
   mvn clean test
   
   # 更新版本号（移除SNAPSHOT）
   mvn versions:set -DnewVersion=1.0.2
   mvn versions:set -DnewVersion=1.0.2 -pl d2k-client
   mvn versions:commit
   ```

2. **构建和验证**
   ```bash
   # 清理并构建
   mvn clean package
   
   # 运行所有测试
   mvn test
   ```

3. **发布后准备下一个开发版本**
   ```bash
   # 更新到下一个SNAPSHOT版本
   mvn versions:set -DnewVersion=1.0.3-SNAPSHOT
   mvn versions:set -DnewVersion=1.0.3-SNAPSHOT -pl d2k-client
   mvn versions:commit
   ```

## 配置选项详解

D2K采用配置分离设计，将Kafka原生配置与D2K专有配置分开管理：

### 配置分离原则

- **Kafka原生配置**：所有不以`d2k.`开头的配置项，直接传递给KafkaConsumer
- **D2K专有配置**：所有以`d2k.`开头的配置项，由D2kConsumerConfig类管理

### D2K专有配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `d2k.loop.total.ms` | Long | 200 | 消费循环总时间（毫秒），控制消费者轮询频率 |
| `d2k.queue.capacity` | Integer | 1000 | 内部队列容量阈值，超过此值将暂停分区消费 |
| `d2k.pause.threshold` | Double | 0.8 | 暂停阈值，队列使用率超过此值时暂停分区 |

### Kafka原生配置

支持所有标准Kafka消费者配置，包括但不限于：

| 配置项 | 说明 |
|--------|------|
| `bootstrap.servers` | Kafka集群地址 |
| `group.id` | 消费者组ID |
| `client.id` | 客户端ID |
| `auto.offset.reset` | 偏移量重置策略 |
| `session.timeout.ms` | 会话超时时间 |
| `heartbeat.interval.ms` | 心跳间隔 |
| `max.poll.records` | 单次拉取最大记录数 |
| `max.poll.interval.ms` | 拉取间隔 |

#### 重要说明：enable.auto.commit 参数

**`enable.auto.commit` 参数在 D2K 中具有特殊性：**
- **默认值**：`false`
- **可修改性**：不可修改，系统会强制设置为 `false`
- **原因**：D2K 延迟消费需要精确控制偏移量提交时机，确保消息处理的可靠性
- **影响**：所有偏移量提交都由 D2K 内部机制自动管理，无需用户干预

### 生产者配置

生产者配置示例：

```java
Map<String, Object> producerProps = new HashMap<>();
// 标准Kafka配置
producerProps.put("bootstrap.servers", "localhost:9092");
producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

// 注意：生产者只需要标准Kafka配置，延迟功能通过API方法实现
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

## 技术实现细节

### 消息延迟标记机制
- **消息头解析**：从Kafka消息头中提取 `d2k-deliver-at` 时间戳，确定消息的预期处理时间
- **时间计算**：支持绝对时间戳和相对延迟时间两种方式，通过extractResumeAtMillis方法统一处理
- **DelayItem封装**：将ConsumerRecord封装为DelayItem对象，包含延迟时间、到期时间戳和原始消息记录

### 双线程处理机制
- **DelayConsumerRunnable主线程**：负责从Kafka拉取消息，解析延迟时间，将DelayItem投递到PriorityBlockingQueue
- **TopicPartitionProcessor处理线程**：每个分区独立的处理线程，从队列中获取到期消息并执行业务逻辑
- **队列管理**：基于PriorityBlockingQueue实现按时间排序，未到期消息等待，到期消息立即处理
- **分区暂停**：当队列容量达到阈值时，暂停对应分区的消费，实现流控保护

### 精确时间控制
- **优先级队列排序**：DelayItem基于resumeAtTimestamp自然排序，确保最早到期的消息优先处理
- **分段休眠策略**：TopicPartitionProcessor采用最大200ms的分段休眠，平衡延迟精度和线程响应性
- **毫秒级精度**：支持毫秒级的延迟时间控制，通过System.currentTimeMillis()进行时间比较

### 处理模式支持
- **同步处理**：消息处理完成后立即提交偏移量，保证强一致性
- **异步处理**：通过线程池异步处理消息，提升并发能力

### 架构设计原理

**双线程异步架构**：
- **消费线程**：DelayConsumerRunnable负责从Kafka拉取消息并解析延迟时间
- **处理线程**：TopicPartitionProcessor负责处理到期的延迟消息
- **线程间异步解耦**：通过PriorityBlockingQueue实现生产者-消费者模式

**时间控制算法**：
- **优先级队列排序**：基于DelayItem的resumeAtTimestamp进行自然排序
- **分段时间控制**：采用多级休眠机制，在不同时间窗口内使用不同的检查频率

**一致性保障机制**：
- **偏移量管理**：同步模式立即提交，异步模式通过nextExpectedOffset和completedItems维护提交顺序
- **背压控制**：队列容量限制和分区暂停机制，防止内存溢出并提供流控能力
- **故障恢复**：支持消费者重平衡和优雅关闭，确保消息处理的可靠性

## API 接口文档

### 核心接口

#### DelayProducer

延迟消息生产者，提供发送延迟消息的能力。

```java
public class DelayProducer<K, V> {
    // 构造方法
    public DelayProducer(Map<String, Object> configs)
    
    // 发送延迟消息（相对延迟时间）
    public Future<RecordMetadata> sendDelayMessage(String topic, K key, V value, long delayMs)
    
    // 发送定时消息（绝对时间）
    public Future<RecordMetadata> sendTimedMessage(String topic, K key, V value, long deliverAtTimestamp)
    
    // 关闭生产者
    public void close()
}
```

#### DelayItemHandler

消息处理器接口，用户需要实现此接口来处理延迟消息。

```java
public interface DelayItemHandler<K, V> {
    void process(DelayItem<K, V> item);
}
```

#### DelayConsumerContainer

延迟消息消费者容器，管理多个消费者线程。

```java
public class DelayConsumerContainer<K, V> {
    // 构造方法
    public DelayConsumerContainer(int threadCount, 
                                 Map<String, Object> consumerProps,
                                 List<String> topics,
                                 DelayItemHandler<K, V> handler)
    
    // 启动容器
    public void start()
    
    // 停止容器
    public void stop()
}
```

### 配置类

#### AsyncProcessingConfig

异步处理配置类。

```java
public class AsyncProcessingConfig {
    // 工厂方法
    public static AsyncProcessingConfig createAsyncConfig(int corePoolSize, 
                                                         int maximumPoolSize, 
                                                         int queueCapacity)
    
    // 配置方法
    public void setEnabled(boolean enabled)
    public void setCorePoolSize(int corePoolSize)
    public void setMaximumPoolSize(int maximumPoolSize)
    public void setQueueCapacity(int queueCapacity)
    public void setRejectedExecutionPolicy(RejectedExecutionPolicy policy)
}
```

## 贡献流程

### 开发工作流

#### 1. Fork 项目

1. 访问 [D2K 项目主页](https://github.com/xiajuan96/d2k)
2. 点击右上角的 "Fork" 按钮
3. 将项目 Fork 到你的 GitHub 账户

#### 2. 创建开发分支

```bash
# 克隆你的 Fork
git clone https://github.com/YOUR_USERNAME/d2k.git
cd d2k

# 添加上游仓库
git remote add upstream https://github.com/xiajuan96/d2k.git

# 创建功能分支
git checkout -b feature/your-feature-name
```

#### 3. 开发和测试

```bash
# 进行代码开发
# ...

# 运行测试确保代码质量
mvn clean test

# 运行代码格式检查
mvn checkstyle:check
```

#### 4. 提交代码

```bash
# 添加修改的文件
git add .

# 提交代码（遵循提交信息规范）
git commit -m "feat: add new delay processing feature"

# 推送到你的 Fork
git push origin feature/your-feature-name
```

#### 5. 创建 Pull Request

1. 访问你的 Fork 页面
2. 点击 "Compare & pull request"
3. 填写 PR 描述，说明你的修改内容
4. 等待代码审查和合并

### 提交信息规范

使用 [Conventional Commits](https://www.conventionalcommits.org/) 规范：

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

**类型说明**：
- `feat`: 新功能
- `fix`: 修复 bug
- `docs`: 文档更新
- `style`: 代码格式调整
- `refactor`: 代码重构
- `test`: 测试相关
- `chore`: 构建过程或辅助工具的变动

**示例**：
```
feat(consumer): add async processing support

fix(producer): resolve message header encoding issue

docs: update API documentation for DelayProducer
```

### 代码审查流程

1. **自动检查**：PR 会触发自动化测试和代码质量检查
2. **人工审查**：项目维护者会审查代码逻辑和设计
3. **反馈处理**：根据审查意见修改代码
4. **合并**：审查通过后合并到主分支

## 开发规范

### 代码风格

#### 基本规范
- 遵循 Java 标准编码规范
- 使用 4 个空格进行缩进，不使用 Tab
- 行长度不超过 120 字符
- 文件编码使用 UTF-8

#### 命名规范
- **类名**：使用 PascalCase（如：`DelayProducer`）
- **方法名和变量名**：使用 camelCase（如：`sendDelayMessage`）
- **常量**：使用 UPPER_SNAKE_CASE（如：`DEFAULT_TIMEOUT_MS`）
- **包名**：使用小写字母，用点分隔（如：`io.github.xiajuan96.d2k`）

#### 代码组织
- 导入语句按字母顺序排列
- 静态导入放在普通导入之后
- 类成员按以下顺序排列：
  1. 静态常量
  2. 实例变量
  3. 构造方法
  4. 公共方法
  5. 私有方法

### 注释规范

#### Javadoc 注释
- 所有公共类必须有类级别的 Javadoc
- 所有公共方法必须有方法级别的 Javadoc
- 参数和返回值必须有 `@param` 和 `@return` 说明
- 异常情况必须有 `@throws` 说明

#### 类注释示例
```java
/**
 * D2K 延迟消息生产者，提供发送延迟消息的能力。
 * 
 * <p>支持两种延迟方式：
 * <ul>
 *   <li>相对延迟时间：基于当前时间的延迟</li>
 *   <li>绝对时间：指定具体的执行时间</li>
 * </ul>
 * 
 * @author xiajuan96
 * @since 1.0.0
 */
public class DelayProducer<K, V> {
    // ...
}
```

#### 方法注释示例
```java
/**
 * 发送延迟消息到指定主题。
 * 
 * @param topic 目标主题名称
 * @param key 消息键
 * @param value 消息值
 * @param delayMs 延迟时间（毫秒）
 * @return 发送结果的 Future 对象
 * @throws IllegalArgumentException 当延迟时间为负数时
 */
public Future<RecordMetadata> sendDelayMessage(String topic, K key, V value, long delayMs) {
    // ...
}
```

#### 行内注释
- 复杂逻辑必须添加行内注释
- 注释应该解释"为什么"而不是"是什么"
- 使用中文注释，保持简洁明了

### 测试要求

#### 测试结构
- 所有测试类必须放在 `d2k-test` 模块中
- 测试类命名：`被测试类名 + Test`（如：`DelayProducerTest`）
- 集成测试命名：`功能名 + IntegrationTest`

#### 测试覆盖率
- 新功能必须包含单元测试
- 代码覆盖率不低于 80%
- 核心功能覆盖率不低于 90%

#### 测试分类
- **单元测试**：测试单个类或方法的功能
- **集成测试**：测试多个组件协作的场景
- **性能测试**：验证关键路径的性能指标

#### 测试示例
```java
/**
 * DelayProducer 单元测试
 * 
 * @author xiajuan96
 */
class DelayProducerTest {
    
    @Test
    void shouldSendDelayMessageSuccessfully() {
        // Given
        DelayProducer<String, String> producer = new DelayProducer<>(configs);
        
        // When
        Future<RecordMetadata> result = producer.sendDelayMessage("test-topic", "key", "value", 1000L);
        
        // Then
        assertThat(result).isNotNull();
        // 更多断言...
    }
}
```

### 异常处理

- 使用具体的异常类型，避免使用 `Exception`
- 异常信息要清晰描述问题和解决建议
- 不要忽略异常，至少要记录日志
- 在方法签名中声明可能抛出的检查异常

### 日志规范

- 使用 SLF4J 作为日志门面
- 日志级别使用规范：
  - `ERROR`：系统错误，需要立即处理
  - `WARN`：警告信息，可能影响功能
  - `INFO`：重要的业务流程信息
  - `DEBUG`：调试信息，生产环境关闭
- 避免在循环中打印大量日志
- 敏感信息不要记录到日志中

## 许可证信息

本项目采用 [GNU Lesser General Public License v3.0 (LGPL-3.0)](https://www.gnu.org/licenses/lgpl-3.0.html) 开源许可证。

### 许可证要点

LGPL-3.0 是一个宽松的开源许可证，允许您：
- 自由使用、修改和分发本软件
- 在商业项目中使用本软件
- 将本软件作为库链接到您的应用程序中

### 义务和限制

如果您修改了本软件的源代码并分发，则必须在相同的 LGPL-3.0 许可证下提供修改后的源代码。

### 作者信息

- **项目维护者**：xiajuan96
- **GitHub**：[https://github.com/xiajuan96](https://github.com/xiajuan96)
- **联系方式**：通过GitHub Issues或Pull Request

---

## 相关文档

更多详细信息和使用示例，请参考项目的其他文档：
- [README.md](README.md) - 项目概述和快速开始
- [ADVANCED_USAGE.md](ADVANCED_USAGE.md) - 高级使用指南和配置详解
- [FAQ.md](FAQ.md) - 常见问题解答