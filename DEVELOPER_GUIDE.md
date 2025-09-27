# D2K 开发者指南

本文档为 D2K (Delay to Kafka) 项目的开发者提供详细的技术信息，包括版本管理、环境配置、API使用说明和开发规范等。

## 目录

- [系统要求](#系统要求)
- [环境配置](#环境配置)
- [贡献流程](#贡献流程)
- [开发规范](#开发规范)
- [版本管理](#版本管理)

## 系统要求

### 基础环境

- **Java版本**：JDK 8 或更高版本
- **Kafka版本**：兼容 Apache Kafka 2.0+ 
- **依赖管理**：Maven 3.6+ 或 Gradle 6.0+
- **框架依赖**：无（不依赖 Spring 等框架）

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

类级别的Javadoc注释应包含类的主要功能描述、支持的特性列表、使用场景说明等。注释应使用标准的Javadoc标签，包括@author标识作者信息，@since标识版本信息。对于复杂的类，可以使用HTML标签来组织内容结构，提高可读性。

#### 方法注释示例

方法级别的Javadoc注释应详细描述方法的功能、参数含义、返回值说明和可能抛出的异常。每个参数都应使用@param标签进行说明，返回值使用@return标签描述，异常情况使用@throws标签标明。注释应清晰说明方法的前置条件、后置条件和副作用。

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

测试类应遵循标准的单元测试结构，包含完整的测试生命周期管理。每个测试方法应包含清晰的Given-When-Then结构，确保测试的可读性和可维护性。测试应覆盖正常流程、边界条件和异常情况，并使用合适的断言库进行结果验证。

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

## 相关文档

### 使用指南
- [README](./README.md) - 项目概览和快速开始
- [高级用法](./ADVANCED_USAGE.md) - 详细配置和最佳实践
- [配置分离指南](./CONFIG_SEPARATION_GUIDE.md) - D2K配置机制详解

### 运维指南
- [性能调优指南](./PERFORMANCE_TUNING.md) - 性能优化和调优策略
- **监控功能说明**：当前版本暂不提供监控支持功能，监控能力已规划为未来版本的开发计划

### 问题解决
- [常见问题](./FAQ.md) - 开发和部署常见问题