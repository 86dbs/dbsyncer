# 异构数据库DDL同步实施方案

## 1. 需求背景

当前DBSyncer项目仅支持同类型数据库间的DDL同步（如MySQL→MySQL、SQL Server→SQL Server），暂不支持跨数据库类型的DDL同步。为了满足更广泛的业务场景需求，需要实现异构数据库间的DDL同步功能。

## 2. 设计目标

### 2.1 核心目标
- 支持跨数据库类型的DDL同步（如MySQL↔SQL Server、MySQL↔PostgreSQL等）
- 保持与现有同构DDL同步功能的兼容性
- 最小化对现有架构的改动

### 2.2 设计原则
- 遵循奥卡姆剃刀原则，最小化不必要的复杂性
- 复用现有基础设施，避免重复造轮子
- 保证类型转换的一致性和准确性

## 3. 技术架构

### 3.1 现有架构分析
当前DDL同步处理流程：
```
DDLChangedEvent → DDLParser解析SQL → 表名替换 → 操作类型识别 → 生成DDLConfig
DDLConfig → 目标连接器执行DDL → 添加防循环标识 → 返回执行结果
```

限制条件：
- 在[GeneralBufferActuator.parseDDl](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/flush/impl/GeneralBufferActuator.java#L212-L260)方法中限制了源和目标连接器类型必须相同

### 3.2 异构DDL同步架构
```
DDLChangedEvent → DDLParser解析SQL → 表名替换 → 操作类型识别 → 
异构语法转换 → 生成目标DDLConfig → 目标连接器执行DDL → 返回执行结果
```

## 4. 实现方案

### 4.1 移除同源限制
修改[GeneralBufferActuator.parseDDl](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/flush/impl/GeneralBufferActuator.java#L212-L260)方法，移除连接器类型必须相同的限制：

```java
// 原有代码：
if (mapping.getListener().isEnableDDL() && StringUtil.equals(sConnType, tConnType)) {
    // 只有当源和目标连接器类型相同时才执行DDL同步
}

// 修改后代码：
if (mapping.getListener().isEnableDDL()) {
    // 支持异构数据库DDL同步
}
```

### 4.2 增强DDL解析器
在[DDLParserImpl](file://e:\github\dbsyncer\dbsyncer-parser\src\main\java\org\dbsyncer\parser\ddl\impl\DDLParserImpl.java#L46-L195)中增加异构数据库语法转换逻辑：

1. 识别源数据库类型
2. 根据目标数据库类型进行语法转换
3. 利用SchemaResolver进行类型映射转换

### 4.3 语法转换策略
针对不同数据库间的语法差异，实现转换策略：

#### 4.3.1 字段添加语法转换
- MySQL: `ALTER TABLE table_name ADD COLUMN column_name type`
- SQL Server: `ALTER TABLE table_name ADD column_name type`

#### 4.3.2 字段修改语法转换
- MySQL: `ALTER TABLE table_name MODIFY COLUMN column_name type`
- SQL Server: `ALTER TABLE table_name ALTER COLUMN column_name type`

#### 4.3.3 字段重命名语法转换
- MySQL: `ALTER TABLE table_name CHANGE COLUMN old_name new_name type`
- SQL Server: `sp_rename 'table_name.old_name', 'new_name', 'COLUMN'`

### 4.4 类型映射转换
利用现有的[SchemaResolver](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/schema/SchemaResolver.java#L12-L51)架构实现类型转换：

1. 使用源数据库的SchemaResolver将字段类型标准化
2. 使用目标数据库的SchemaResolver将标准类型转换为目标数据库特定类型

示例：
```java
// MySQL的VARCHAR(50) → 标准类型STRING → SQL Server的NVARCHAR(50)
```

## 5. 实现步骤

### 5.1 第一阶段：基础设施准备
1. 修改[GeneralBufferActuator](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/flush/impl/GeneralBufferActuator.java#L46-L286)移除同源限制
2. 扩展[DDLConfig](file://e:\github\dbsyncer\dbsyncer-sdk\src\main\java\org\dbsyncer\sdk\config\DDLConfig.java#L9-L55)增加源数据库类型信息
3. 创建异构DDL转换器接口和实现类

### 5.2 第二阶段：语法转换实现
1. 实现各数据库间的语法转换策略
   - [MySQLToSQLServerConverter](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/impl/MySQLToSQLServerConverter.java#L22-L195)
   - [SQLServerToMySQLConverter](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/impl/SQLServerToMySQLConverter.java#L22-L166)
2. 集成到[DDLParserImpl](file://e:\github\dbsyncer\dbsyncer-parser\src\main\java\org\dbsyncer\parser\ddl\impl\DDLParserImpl.java#L46-L195)
3. 处理特殊语法（如SQL Server的sp_rename存储过程）

### 5.3 第三阶段：类型映射实现
1. 利用现有[SchemaResolver](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/schema/SchemaResolver.java#L12-L51)实现类型转换
2. 验证各数据库间的类型映射准确性
3. 处理特殊类型（如SQL Server的identity、MySQL的auto_increment等）

### 5.4 第四阶段：测试验证
1. 扩展现有测试用例覆盖异构场景
2. 验证各数据库组合的DDL同步功能
3. 性能测试和异常处理验证

## 6. 已实现功能

### 6.1 核心组件
1. [HeterogeneousDDLConverter](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/HeterogeneousDDLConverter.java#L12-L37)接口：定义异构DDL转换器规范
2. [HeterogeneousDDLConverterImpl](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/impl/HeterogeneousDDLConverterImpl.java#L14-L76)：异构DDL转换器实现
3. [MySQLToSQLServerConverter](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/impl/MySQLToSQLServerConverter.java#L22-L195)：MySQL到SQL Server的转换器
4. [SQLServerToMySQLConverter](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/impl/SQLServerToMySQLConverter.java#L22-L166)：SQL Server到MySQL的转换器

### 6.2 功能增强
1. [DDLConfig](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/config/DDLConfig.java#L9-L73)扩展：增加源和目标连接器类型字段
2. [GeneralBufferActuator](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/flush/impl/GeneralBufferActuator.java#L46-L286)修改：移除同源限制，支持异构DDL同步
3. [DDLParserImpl](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/impl/DDLParserImpl.java#L46-L195)增强：集成异构DDL转换功能

## 7. 风险评估与应对

### 7.1 技术风险
1. **语法兼容性问题**：不同数据库的DDL语法差异较大
   - 应对：建立完整的语法映射表，针对每种操作类型实现转换策略

2. **类型映射不准确**：异构数据库间的数据类型不完全对应
   - 应对：基于现有SchemaResolver架构，确保类型转换一致性

3. **特殊语法处理**：某些操作在目标数据库中需要特殊处理
   - 应对：实现特殊语法处理机制，如SQL Server的sp_rename

### 7.2 兼容性风险
1. **对现有功能的影响**：修改可能影响现有的同构DDL同步
   - 应对：确保向后兼容，增加充分的测试用例

## 8. 测试策略

### 8.1 测试范围
1. MySQL ↔ SQL Server
2. MySQL ↔ PostgreSQL
3. SQL Server ↔ PostgreSQL

### 8.2 测试场景
1. 字段添加操作
2. 字段修改操作
3. 字段重命名操作
4. 字段删除操作
5. 异常情况处理

### 8.3 测试用例示例
```sql
-- MySQL到SQL Server的ADD COLUMN
-- 源(MySQL): ALTER TABLE test_table ADD COLUMN salary DECIMAL(10,2)
-- 目标(SQL Server): ALTER TABLE test_table ADD salary DECIMAL(10,2)

-- SQL Server到MySQL的ALTER COLUMN
-- 源(SQL Server): ALTER TABLE test_table ALTER COLUMN name NVARCHAR(100)
-- 目标(MySQL): ALTER TABLE test_table MODIFY COLUMN name VARCHAR(100)
```

## 9. 性能与监控

### 9.1 性能考虑
1. 语法转换应尽量轻量，避免影响同步性能
2. 复用现有缓存机制，减少重复解析
3. 异步处理复杂转换操作

### 9.2 监控指标
1. DDL转换成功率
2. 类型映射准确率
3. 异常处理统计

## 10. 部署与升级

### 10.1 部署方案
1. 先在测试环境验证功能
2. 逐步在生产环境启用
3. 提供配置开关控制功能启用

### 10.2 升级注意事项
1. 确保向后兼容性
2. 提供升级文档和迁移指南
3. 监控升级后的运行状态

## 11. 总结

通过以上方案，已经实现了DBSyncer对异构数据库DDL同步的支持，在保持现有架构稳定性的前提下，扩展了系统的适用范围。实现过程中重点关注了语法转换的准确性和类型映射的一致性，充分利用了现有基础设施，确保了功能的稳定性和可靠性。

目前已完成核心功能的开发，包括异构DDL转换器接口和实现、语法转换策略、类型映射等。下一步需要完善测试用例，验证各数据库组合的DDL同步功能，并进行性能测试和异常处理验证。