# D2K消费者配置分离指南

本文档详细说明了D2K延迟消息框架中消费者配置的分离机制，基于`D2kConsumerConfig`类的实际实现，帮助开发者理解如何正确配置和使用Kafka原生配置与D2K专有配置。

## 配置分离原理

D2K框架采用配置分离的设计理念，通过`D2kConsumerConfig`类实现配置的自动分离和管理：

1. **Kafka原生配置**：直接传递给`KafkaConsumer`，控制Kafka客户端行为
2. **D2K专有配置**：由`D2kConsumerConfig`类解析和管理，控制延迟处理逻辑

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

基于`D2kConsumerConfig`类的实际实现：

| 配置项 | 类型 | 默认值 | 常量定义 | 说明 |
|--------|------|--------|----------|------|
| `d2k.loop.total.ms` | Long | 200 | `LOOP_TOTAL_MS_CONFIG` | 延迟检查循环的总时间间隔（毫秒） |
| `d2k.queue.capacity` | Integer | 1000 | `QUEUE_CAPACITY_CONFIG` | 延迟消息队列的最大容量 |

**注意**：当前版本仅支持上述两个D2K专有配置项，同步/异步消费功能通过`AsyncProcessingConfig`单独配置。

## D2kConsumerConfig

`D2kConsumerConfig`是D2K框架的核心配置类。

## 向后兼容性

现有代码无需修改，配置拆分在内部自动进行：
- 所有以 `d2k.` 开头的配置项自动归类为D2K专有配置
- 其他所有配置项自动归类为Kafka原生配置
- 原有的构造函数和API保持不变

## 相关文档

### 使用指南
- [README](./README.md) - 项目概览和快速开始
- [高级用法](./ADVANCED_USAGE.md) - 详细配置和最佳实践

### 开发指南
- [开发指南](./DEVELOPER_GUIDE.md) - 完整API文档和开发规范

### 运维指南
- [性能调优指南](./PERFORMANCE_TUNING.md) - 系统性能优化策略
- **监控功能说明**：当前版本暂不提供监控支持功能，监控能力已规划为未来版本的开发计划

### 问题解决
- [常见问题](./FAQ.md) - 配置相关问题排查和解决方案