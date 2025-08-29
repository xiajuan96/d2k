# D2K 日志配置指南

## 概述

D2K Spring Boot Starter 提供了灵活的日志配置机制，默认情况下只输出 WARNING 及以上级别的日志，同时为关键业务模块提供了可配置的 DEBUG 日志开关。

## 默认日志级别

- **根日志级别**: WARN
- **D2K核心组件**: INFO
- **D2K业务逻辑**: INFO/WARN
- **第三方组件**: ERROR
- **测试环境**: ERROR

## DEBUG 日志开关

### 环境变量配置

可以通过以下环境变量来启用特定模块的 DEBUG 日志：

```bash
# 启用示例应用的DEBUG日志
export D2K_EXAMPLE_DEBUG_ENABLED=true

# 启用生产者的DEBUG日志
export D2K_PRODUCER_DEBUG_ENABLED=true

# 启用消费者的DEBUG日志
export D2K_CONSUMER_DEBUG_ENABLED=true

# 启用所有D2K模块的DEBUG日志
export D2K_DEBUG_ENABLED=true
```

### JVM 系统属性配置

也可以通过 JVM 系统属性来配置：

```bash
# 启动时指定系统属性
java -DD2K_EXAMPLE_DEBUG_ENABLED=true -jar your-application.jar

# 或者在Maven运行时指定
mvn exec:java -DD2K_CONSUMER_DEBUG_ENABLED=true
```

### Spring Boot 配置文件

在 `application.yml` 或 `application.properties` 中配置：

```yaml
# application.yml
logging:
  level:
    com.d2k.spring.boot.autoconfigure.example: DEBUG  # 示例应用
    com.d2k.producer: DEBUG                           # 生产者
    com.d2k.consumer: DEBUG                           # 消费者
```

```properties
# application.properties
logging.level.com.d2k.spring.boot.autoconfigure.example=DEBUG
logging.level.com.d2k.porducer=DEBUG
logging.level.com.d2k.consumer=DEBUG
```

## 日志输出格式

### 标准日志格式
```
HH:mm:ss.SSS LEVEL LoggerName - Message
```

### DEBUG 日志格式
```
HH:mm:ss.SSS [DEBUG] LoggerName - Message
```

### 文件日志格式（可选）
```
yyyy-MM-dd HH:mm:ss.SSS [thread] LEVEL LoggerName - Message
```

## 文件日志配置

默认情况下，文件日志是禁用的。要启用文件日志，请在 `logback.xml` 中取消注释以下行：

```xml
<!-- <appender-ref ref="FILE"/> -->
```

文件日志配置：
- **文件位置**: `logs/d2k-application.log`
- **滚动策略**: 按日期和大小滚动
- **最大文件大小**: 10MB
- **保留天数**: 30天
- **总大小限制**: 300MB
- **日志级别**: INFO及以上

## 使用示例

### 1. 日常运行（默认配置）
```bash
# 只输出WARN及以上级别的日志
mvn exec:java
```

### 2. 调试示例应用
```bash
# 启用示例应用的DEBUG日志
export D2K_EXAMPLE_DEBUG_ENABLED=true
mvn exec:java
```

### 3. 调试消费者问题
```bash
# 启用消费者的DEBUG日志
mvn exec:java -DD2K_CONSUMER_DEBUG_ENABLED=true
```

### 4. 全面调试
```bash
# 启用所有模块的DEBUG日志
export D2K_EXAMPLE_DEBUG_ENABLED=true
export D2K_PRODUCER_DEBUG_ENABLED=true
export D2K_CONSUMER_DEBUG_ENABLED=true
mvn exec:java
```

## 测试环境配置

在测试环境中，日志级别更加严格：
- **根日志级别**: ERROR
- **D2K核心组件**: WARN
- **其他组件**: ERROR

激活测试配置：
```bash
java -Dspring.profiles.active=test -jar your-application.jar
```

## 配置优先级

日志配置的优先级从高到低：
1. JVM 系统属性 (`-D` 参数)
2. 环境变量
3. Spring Boot 配置文件 (`application.yml`/`application.properties`)
4. Logback 配置文件 (`logback.xml`)
5. 默认配置

## 注意事项

1. **性能影响**: DEBUG 日志会影响性能，建议仅在需要调试时启用
2. **日志量**: 启用 DEBUG 日志会产生大量日志输出，注意磁盘空间
3. **生产环境**: 生产环境建议使用默认配置，避免启用 DEBUG 日志
4. **动态配置**: 某些配置需要重启应用才能生效

## 故障排除

### 问题：DEBUG 日志没有输出
**解决方案**：
1. 检查环境变量是否正确设置
2. 确认 JVM 系统属性是否正确传递
3. 验证 Spring Boot 配置文件中的日志级别设置
4. 检查 logback.xml 配置是否正确

### 问题：日志输出过多
**解决方案**：
1. 关闭不必要的 DEBUG 开关
2. 调整特定包的日志级别
3. 使用文件日志而不是控制台输出
4. 配置日志过滤器

### 问题：文件日志没有生成
**解决方案**：
1. 确认文件 appender 已启用
2. 检查文件路径权限
3. 验证日志级别配置
4. 查看应用启动日志中的错误信息