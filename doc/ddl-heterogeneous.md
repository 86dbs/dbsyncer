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

### 3.2 异构DDL同步架构（中间表示方案）
```
DDLChangedEvent → DDLParser解析SQL → 表名替换 → 操作类型识别 → 
源数据库DDL → [源ToIRConverter] → 中间表示(IR) → [IRTo目标Converter] → 目标数据库DDL → 生成DDLConfig → 
目标连接器执行DDL → 返回执行结果
```

## 4. 中间表示(IR)设计

### 4.1 统一中间表示结构
```java
public class DDLIntermediateRepresentation {
    private String tableName;
    private DDLOperationType operationType; // ADD, MODIFY, DROP, CHANGE
    private List<Field> columns;  // 直接复用现有的Field类
}
```

### 4.2 转换流程
```
源数据库DDL → [源ToIRConverter] → 中间表示(IR) → [IRTo目标Converter] → 目标数据库DDL
```

## 5. 实现方案

### 5.1 移除同源限制
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

### 5.2 增强DDL解析器
在[DDLParserImpl](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/impl/DDLParserImpl.java#L46-L222)中增加异构数据库语法转换逻辑：

1. 识别源数据库类型
2. 将源DDL转换为中间表示(IR)
3. 将中间表示(IR)转换为目标数据库DDL
4. 利用SchemaResolver进行类型映射转换

### 5.3 中间表示转换策略
针对不同数据库间的语法差异，实现转换策略：

#### 5.3.1 字段添加语法转换
- MySQL: `ALTER TABLE table_name ADD COLUMN column_name type`
- SQL Server: `ALTER TABLE table_name ADD column_name type`

#### 5.3.2 字段修改语法转换
- MySQL: `ALTER TABLE table_name MODIFY COLUMN column_name type`
- SQL Server: `ALTER TABLE table_name ALTER COLUMN column_name type`

#### 5.3.3 字段重命名语法转换
- MySQL: `ALTER TABLE table_name CHANGE COLUMN old_name new_name type`
- SQL Server: `sp_rename 'table_name.old_name', 'new_name', 'COLUMN'`

### 5.4 类型映射转换
利用现有的[SchemaResolver](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/schema/SchemaResolver.java#L12-L51)架构实现类型转换：

1. 使用源数据库的SchemaResolver将字段类型标准化
2. 使用目标数据库的SchemaResolver将标准类型转换为目标数据库特定类型

示例：
```java
// MySQL的VARCHAR(50) → 标准类型STRING → SQL Server的NVARCHAR(50)
```

## 6. 实现步骤

### 6.1 第一阶段：基础设施准备
1. 修改[GeneralBufferActuator](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/flush/impl/GeneralBufferActuator.java#L46-L286)移除同源限制
2. 扩展[DDLConfig](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/config/DDLConfig.java#L9-L73)增加源数据库类型信息
3. 创建中间表示(IR)相关类
4. 创建异构DDL转换器接口和实现类

### 6.2 第二阶段：中间表示转换实现
1. 实现各数据库到IR的转换器
   - [MySQLToIRConverter](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/converter/MySQLToIRConverter.java#L34-L171)
   - [SQLServerToIRConverter](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/converter/SQLServerToIRConverter.java#L33-L169)
2. 实现IR到各数据库的转换器
   - [IRToMySQLConverter](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/converter/IRToMySQLConverter.java#L22-L161)
   - [IRToSQLServerConverter](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/converter/IRToSQLServerConverter.java#L22-L163)
3. 集成到[DDLParserImpl](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/impl/DDLParserImpl.java#L46-L222)

### 6.3 第三阶段：类型映射实现
1. 利用现有[SchemaResolver](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/schema/SchemaResolver.java#L12-L51)实现类型转换
2. 验证各数据库间的类型映射准确性
3. 处理特殊类型（如SQL Server的identity、MySQL的auto_increment等）

### 6.4 第四阶段：测试验证
1. 扩展现有测试用例覆盖异构场景
2. 验证各数据库组合的DDL同步功能
3. 性能测试和异常处理验证

## 7. 已实现功能

### 7.1 核心组件
1. [HeterogeneousDDLConverter](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/HeterogeneousDDLConverter.java#L12-L37)接口：定义异构DDL转换器规范
2. [HeterogeneousDDLConverterImpl](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/impl/HeterogeneousDDLConverterImpl.java#L14-L76)：异构DDL转换器实现
3. [MySQLToIRConverter](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/converter/MySQLToIRConverter.java#L34-L171)：MySQL到中间表示的转换器
4. [SQLServerToIRConverter](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/converter/SQLServerToIRConverter.java#L33-L169)：SQL Server到中间表示的转换器
5. [IRToMySQLConverter](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/converter/IRToMySQLConverter.java#L22-L161)：中间表示到MySQL的转换器
6. [IRToSQLServerConverter](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/converter/IRToSQLServerConverter.java#L22-L163)：中间表示到SQL Server的转换器

### 7.2 功能增强
1. [DDLConfig](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/config/DDLConfig.java#L9-L73)扩展：增加源和目标连接器类型字段
2. [GeneralBufferActuator](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/flush/impl/GeneralBufferActuator.java#L46-L286)修改：移除同源限制，支持异构DDL同步
3. [DDLParserImpl](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/impl/DDLParserImpl.java#L46-L222)增强：集成异构DDL转换功能

## 8. 新方案改进点

### 8.1 中间表示(IR)架构优势
1. **高度解耦**：每个数据库只需关注与IR的转换
2. **易于扩展**：新增数据库只需实现两个转换方法
3. **类型一致性**：通过标准类型确保类型转换的一致性
4. **复用现有基础设施**：直接复用现有的Field类和DataTypeEnum枚举
5. **维护简单**：修改某个数据库的转换逻辑不影响其他数据库

### 8.2 SQL Template改进
1. **SQL生成更直观**：将数据库特定的SQL生成逻辑集中到对应的Template类中
2. **职责更清晰**：IRToXXX转换器负责流程控制，Template负责SQL生成
3. **易于维护**：所有数据库特定的SQL生成逻辑都在一个地方维护

### 8.3 DDLParserImpl修复
1. **移除演示代码**：修复了之前硬编码"MySQL"作为源连接器类型的演示代码
2. **正确获取连接器类型**：通过TableGroup的ProfileComponent正确获取源和目标连接器类型
3. **增强健壮性**：确保在生产环境中能正确识别和处理异构数据库DDL
4. **改善异常处理**：当没有converter或不支持转换时抛出明确的异常，而不是静默失败

### 8.4 设计模式应用
1. **策略模式**：不同数据库的转换策略独立实现
2. **中介者模式**：IR作为中介者协调不同数据库间的转换
3. **工厂模式**：通过工厂创建具体的转换器实例

## 9. 风险评估与应对

### 9.1 技术风险
1. **语法兼容性问题**：不同数据库的DDL语法差异较大
   - 应对：建立完整的语法映射表，针对每种操作类型实现转换策略

2. **类型映射不准确**：异构数据库间的数据类型不完全对应
   - 应对：基于现有SchemaResolver架构，确保类型转换一致性

3. **特殊语法处理**：某些操作在目标数据库中需要特殊处理
   - 应对：实现特殊语法处理机制，如SQL Server的sp_rename

### 9.2 兼容性风险
1. **对现有功能的影响**：修改可能影响现有的同构DDL同步
   - 应对：确保向后兼容，增加充分的测试用例

## 10. 测试策略

### 10.1 测试范围
1. MySQL ↔ SQL Server
2. MySQL ↔ PostgreSQL
3. SQL Server ↔ PostgreSQL

### 10.2 测试场景
1. 字段添加操作
2. 字段修改操作
3. 字段重命名操作
4. 字段删除操作
5. 异常情况处理

### 10.3 测试用例示例
```sql
-- MySQL到SQL Server的ADD COLUMN
-- 源(MySQL): ALTER TABLE test_table ADD COLUMN salary DECIMAL(10,2)
-- 目标(SQL Server): ALTER TABLE test_table ADD salary DECIMAL(10,2)

-- SQL Server到MySQL的ALTER COLUMN
-- 源(SQL Server): ALTER TABLE test_table ALTER COLUMN name NVARCHAR(100)
-- 目标(MySQL): ALTER TABLE test_table MODIFY COLUMN name VARCHAR(100)
```

## 11. 性能与监控

### 11.1 性能考虑
1. 语法转换应尽量轻量，避免影响同步性能
2. 复用现有缓存机制，减少重复解析
3. 异步处理复杂转换操作

### 11.2 监控指标
1. DDL转换成功率
2. 类型映射准确率
3. 异常处理统计

## 12. 部署与升级

### 12.1 部署方案
1. 先在测试环境验证功能
2. 逐步在生产环境启用
3. 提供配置开关控制功能启用

### 12.2 升级注意事项
1. 确保向后兼容性
2. 提供升级文档和迁移指南
3. 监控升级后的运行状态

## 13. 总结

通过以上方案，已经实现了DBSyncer对异构数据库DDL同步的支持，在保持现有架构稳定性的前提下，扩展了系统的适用范围。实现过程中重点关注了语法转换的准确性和类型映射的一致性，充分利用了现有基础设施，确保了功能的稳定性和可靠性。

采用中间表示(IR)的架构设计，进一步提高了系统的可扩展性和维护性。通过将源数据库DDL转换为统一的中间表示，再将中间表示转换为目标数据库DDL，实现了高度解耦的设计。

最新的改进将数据库特定的SQL生成逻辑移到了对应的SQL Template类中，使SQL生成更加直观，同时保持了中间表示的价值。这种设计使职责更加清晰，IRToXXX转换器负责流程控制，而Template负责具体的SQL生成。

此外，我们还修复了[DDLParserImpl](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/ddl/impl/DDLParserImpl.java#L46-L222)中的演示代码，确保能正确从上下文中获取源和目标连接器类型，而不是硬编码为"MySQL"。同时，我们改进了异常处理机制，当没有converter或不支持转换时会抛出明确的异常，而不是静默失败。

目前已完成核心功能的开发，包括异构DDL转换器接口和实现、语法转换策略、类型映射等。下一步需要完善测试用例，验证各数据库组合的DDL同步功能，并进行性能测试和异常处理验证。