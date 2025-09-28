# DQL连接器优化方案

## 背景

在当前的 DBSyncer 项目中，对于同一类型的数据库（如 MySQL、PostgreSQL），存在 `XXXConnector`（非DQL连接器）和 `DQLXXXConnector`（DQL连接器）两种实现。这两类连接器在处理数据库连接、验证、位置获取等基础功能上存在大量重复代码。

## 目标

通过合理的继承设计，消除 `DQLXXXConnector` 和 `XXXConnector` 之间的代码重复，提高代码复用性，简化维护成本，并保持职责清晰。

## 当前架构分析

1.  **`XXXConnector`**：例如 `MySQLConnector`，是具体数据库的连接器实现，负责处理与特定数据库相关的所有操作，包括连接管理、CDC（变更数据捕获）、DDL 解析等。它继承自 `AbstractDatabaseConnector`。
2.  **`DQLXXXConnector`**：例如 `DQLMySQLConnector`，是专门用于 DQL（数据查询语言）场景的连接器。它需要实现基于 SQL 的数据查询逻辑。
3.  **`AbstractDQLConnector`**：这是一个抽象基类，旨在为所有 DQL 连接器提供通用的 DQL 功能，例如 `getTable()` 方法。在新的设计中，这个类将被移除。

## 问题与挑战

-   **代码重复**：`DQLXXXConnector` 和 `XXXConnector` 在基础功能上高度相似，导致大量重复代码。
-   **职责不清**：`AbstractDQLConnector` 的定位模糊，它既是一个抽象类，又包含了部分具体逻辑。

## 优化方案

我们将采用将 AbstractDQLConnector 的功能直接整合到 AbstractDatabaseConnector 中的方案，完全消除 AbstractDQLConnector 抽象层，进一步简化继承结构。

### 核心思想

采用"功能标志模式"，在 AbstractDatabaseConnector 中集成 DQL 功能，并通过配置标志来启用或禁用这些功能。

```
AbstractDatabaseConnector (基础，包含DQL和非DQL功能)
    |
    +-- MySQLConnector (具体数据库实现)
    |
    +-- PostgreSQLConnector (具体数据库实现)
    |
    +-- OracleConnector (具体数据库实现)
```

### 具体实现

1. **增强 AbstractDatabaseConnector**
   - 将 AbstractDQLConnector 中的方法（getTable、getMetaInfo、buildSourceCommands）移到 AbstractDatabaseConnector 中

2. **重构具体连接器**
   - 具体连接器只需继承 AbstractDatabaseConnector，即可自动获得 DQL 和非 DQL 的双重支持
   - 无需在具体连接器中添加额外的 DQL 模式支持代码

3. **更新 DQLXXXConnector**
   - 让 DQLMySQLConnector 直接继承 MySQLConnector
   - 通过配置启用 DQL 模式

4. **删除 AbstractDQLConnector**
   - 完成功能迁移后，删除 AbstractDQLConnector 类

5. **验证连接器注册**
   - DQL连接器类已经通过SPI机制自动注册，无需额外操作

## 预期收益

- **显著减少代码重复**：消除 DQL 和非 DQL 连接器之间的重复代码
- **简化继承结构**：减少不必要的抽象层，使代码结构更清晰
- **提高可维护性**：统一数据库连接器的实现，降低维护成本
- **更好的扩展性**：未来添加新数据库类型时，只需实现一个连接器类

## 结论

通过将 AbstractDQLConnector 的功能整合到 AbstractDatabaseConnector 中，我们可以有效地解决代码重复问题，简化继承层次结构。由于DQL连接器类已经通过SPI机制自动注册，实施该方案时无需额外关注连接器注册问题。