# DBSyncer 覆盖模式性能优化方案

## 1. 概述

当前项目在数据同步时有覆盖模式，该模式当目标数据已经存在时，由 insert 改为 update。这个逻辑没有问题，但在大规模覆盖时，性能瓶颈严重。因此在覆盖模式下直接使用数据库服务器端的能力来提高性能，如 mysql 使用 replace into, mssql 使用 merge into 等。

## 2. 现状分析

### 2.1 当前实现方式

在 [AbstractDatabaseConnector](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L59-L722) 中，当遇到主键冲突时，通过 [forceUpdate](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L658-L666) 方法将 insert 操作转换为 update 操作：

```java
private void forceUpdate(Result result, DatabaseConnectorInstance connectorInstance, PluginContext context, List<Field> pkFields,
                         Map row) {
    String event = context.getEvent();
    if (context.isForceUpdate() && isInsert(event)) {
        // 存在执行覆盖更新，否则写入
        logger.warn("{} {}表执行{}失败, 执行{}, {}", context.getTraceId(), context.getTargetTableName(), event, ConnectorConstant.OPERTION_UPDATE, row);
        writer(result, connectorInstance, context, pkFields, row, ConnectorConstant.OPERTION_UPDATE);
    }
}
```

### 2.2 性能瓶颈

1. **两次数据库交互**：先尝试 insert，失败后再执行 update，增加了网络往返时间
2. **批量处理效率低**：在批量操作中，部分记录需要 update 时，整个批次需要拆分处理
3. **数据库服务器负载**：频繁的主键冲突检查和异常处理增加了数据库服务器负担

## 3. 优化方案设计

### 3.1 核心思想

利用数据库原生的 upsert（update or insert）功能，在单条 SQL 中完成插入或更新操作，避免两次数据库交互。

### 3.2 各数据库实现方案

| 数据库 | Upsert 语法 | 特点 |
|--------|-------------|------|
| MySQL | INSERT ... ON DUPLICATE KEY UPDATE | 标准 SQL，保持记录ID不变 |
| SQL Server | MERGE INTO | 可控制更新和插入逻辑 |
| PostgreSQL | INSERT ... ON CONFLICT | 标准 SQL，功能强大 |
| Oracle | MERGE INTO | 功能完善，语法复杂 |
| SQLite | INSERT OR REPLACE | 简洁语法，完全替换记录 |

### 3.3 实现架构

1. 在 [SqlTemplate](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/sql/SqlTemplate.java#L26-L332) 接口中增加 `buildUpsertSql` 方法
2. 在各数据库特定的模板实现类中实现该方法
3. 在 [AbstractDatabaseConnector](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L59-L722) 中增加对覆盖模式的判断，使用新的 upsert SQL
4. 在 [PluginContext](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/plugin/PluginContext.java#L18-L124) 中增加覆盖模式标识

## 4. 详细实现

### 4.1 SqlTemplate 接口扩展

在 [SqlTemplate](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/sql/SqlTemplate.java#L26-L332) 接口中添加新方法：

```java
/**
 * 构建覆盖插入SQL（Upsert）
 *
 * @param buildContext 构建上下文
 * @return 构建后的SQL（包含?占位符）
 */
String buildUpsertSql(SqlBuildContext buildContext);

/**
 * 构建覆盖插入SQL（Upsert）
 *
 * @param schemaTable  带引号的表名
 * @param fields       字段列表
 * @param primaryKeys  主键列表
 * @return 构建后的SQL（包含?占位符）
 */
String buildUpsertSql(String schemaTable, List<Field> fields, List<String> primaryKeys);

}
```

### 4.2 各数据库实现

#### 4.2.1 MySQL 实现

在 [MySQLTemplate](file:///E:/github/dbsyncer/dbsyncer-connector/dbsyncer-connector-mysql/src/main/java/org/dbsyncer/connector/mysql/MySQLTemplate.java#L22-L31) 中添加：

```java
@Override
public String buildUpsertSql(String schemaTable, List<Field> fields, List<String> primaryKeys) {
    String fieldNames = fields.stream()
            .map(field -> buildColumn(field.getName()))
            .collect(java.util.stream.Collectors.joining(", "));
    String placeholders = fields.stream()
            .map(field -> "?")
            .collect(java.util.stream.Collectors.joining(", "));
    String updateClause = fields.stream()
            .filter(field -> !field.isPk())
            .map(field -> buildColumn(field.getName()) + " = VALUES(" + buildColumn(field.getName()) + ")")
            .collect(java.util.stream.Collectors.joining(", "));
    return String.format("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s", 
                         schemaTable, fieldNames, placeholders, updateClause);
}
```

#### 4.2.2 SQL Server 实现

在 [SqlServerTemplate](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/sql/impl/SqlServerTemplate.java#L22-L77) 中添加：

```java
@Override
public String buildUpsertSql(String schemaTable, List<Field> fields, List<String> primaryKeys) {
    String fieldNames = fields.stream()
            .map(field -> buildColumn(field.getName()))
            .collect(java.util.stream.Collectors.joining(", "));
    String placeholders = fields.stream()
            .map(field -> "?")
            .collect(java.util.stream.Collectors.joining(", "));
    String updateClause = fields.stream()
            .filter(field -> !field.isPk())
            .map(field -> buildColumn(field.getName()) + " = SOURCE." + buildColumn(field.getName()))
            .collect(java.util.stream.Collectors.joining(", "));
    String pkCondition = primaryKeys.stream()
            .map(pk -> "TARGET." + buildColumn(pk) + " = SOURCE." + buildColumn(pk))
            .collect(java.util.stream.Collectors.joining(" AND "));
    
    return String.format(
        "MERGE %s AS TARGET " +
        "USING (SELECT %s) AS SOURCE ON %s " +
        "WHEN MATCHED THEN UPDATE SET %s " +
        "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
        schemaTable, placeholders, pkCondition, updateClause, fieldNames, placeholders);
}
```

### 4.3 AbstractDatabaseConnector 修改

在 [AbstractDatabaseConnector](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L59-L722) 的 [writer](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L208-L279) 方法中添加覆盖模式判断：

```java
@Override
public Result writer(DatabaseConnectorInstance connectorInstance, PluginContext context) {
    String event = context.getEvent();
    List<Map> data = context.getTargetList();
    List<Field> targetFields = context.getTargetFields();

    // 1、获取SQL
    String executeSql;
    if (context.isForceUpdate() && isInsert(event)) {
        // 覆盖模式下使用 Upsert SQL
        executeSql = context.getCommand().get(ConnectorConstant.OPERTION_UPSERT);
    } else {
        // 正常模式下使用原有 SQL
        executeSql = context.getCommand().get(event);
    }
    
    Assert.hasText(executeSql, "执行SQL语句不能为空.");
    if (CollectionUtils.isEmpty(targetFields)) {
        logger.error("writer fields can not be empty.");
        throw new SdkException("writer fields can not be empty.");
    }
    if (CollectionUtils.isEmpty(data)) {
        logger.error("writer data can not be empty.");
        throw new SdkException("writer data can not be empty.");
    }
    
    List<Field> fields = new ArrayList<>(targetFields);
    List<Field> pkFields = PrimaryKeyUtil.findExistPrimaryKeyFields(targetFields);
    // Update / Delete
    if (!isInsert(event)) {
        if (isDelete(event)) {
            fields.clear();
        } else if (isUpdate(event)) {
            removeFieldWithPk(fields, pkFields);
        }
        fields.addAll(pkFields);
    }
    // Upsert操作使用与Insert相同的参数填充逻辑
    // 1. MySQL的INSERT ... ON DUPLICATE KEY UPDATE语法与INSERT语法参数相同
    // 2. SQL Server的MERGE INTO语法虽然需要SOURCE和TARGET两套参数，
    //    但通过在SQL模板中调整占位符顺序，可以适配现有的参数填充结果
    
    Result result = new Result();
    int[] execute = null;
    try {
        // 2、设置参数并执行SQL
        execute = connectorInstance.execute(databaseTemplate -> databaseTemplate.batchUpdate(executeSql, batchRows(fields, data)));
    } catch (Exception e) {
        data.forEach(row -> forceUpdate(result, connectorInstance, context, pkFields, row));
    }

    if (null != execute) {
        int batchSize = execute.length;
        for (int i = 0; i < batchSize; i++) {
            if (execute[i] == 1 || execute[i] == -2) {
                result.getSuccessData().add(data.get(i));
                continue;
            }
            forceUpdate(result, connectorInstance, context, pkFields, data.get(i));
        }
    }
    return result;
}
```

### 4.4 TargetCommand 生成修改

修改 [AbstractDatabaseConnector](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L59-L722) 的 [getTargetCommand](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L339-L377) 方法：

```java
@Override
public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
    // ... 原有代码 ...
    
    // 使用SqlTemplate直接生成SQL
    Map<String, String> map = new HashMap<>();
    map.put(ConnectorConstant.OPERTION_INSERT, sqlTemplate.buildInsertSql(buildContext));
    map.put(ConnectorConstant.OPERTION_UPDATE, sqlTemplate.buildUpdateSql(buildContext));
    map.put(ConnectorConstant.OPERTION_DELETE, sqlTemplate.buildDeleteSql(buildContext));
    
    // 添加 Upsert SQL
    try {
        map.put(ConnectorConstant.OPERTION_UPSERT, sqlTemplate.buildUpsertSql(buildContext));
    } catch (UnsupportedOperationException e) {
        // 如果数据库不支持 Upsert，则不添加该命令
        logger.debug("Upsert not supported for this database: {}", e.getMessage());
    }
    
    return map;
}
```

### 4.5 常量定义

在 [ConnectorConstant](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/constant/ConnectorConstant.java#L12-L48) 中添加：

```java
/**
 * 覆盖插入（Upsert）
 */
public static final String OPERTION_UPSERT = "UPSERT";
```

## 5. 清理工作

在实现Upsert功能后，需要对既有相关代码进行适当的清理和重构：

### 5.1 废弃的代码路径

经过仔细分析，forceUpdate方法在以下两个地方被调用：
1. 在writer方法的异常处理块中：`data.forEach(row -> forceUpdate(result, connectorInstance, context, pkFields, row));`
2. 在writer方法的结果处理块中：`forceUpdate(result, connectorInstance, context, pkFields, data.get(i));`

当启用覆盖模式时，将直接使用Upsert SQL语句，不再需要forceUpdate机制来处理主键冲突。在新的实现中，forceUpdate方法将永远不会被调用，因为：
- 当启用覆盖模式时，会使用新的OPERTION_UPSERT命令
- 当未启用覆盖模式时，forceUpdate方法中的`context.isForceUpdate()`条件为false，不会执行

考虑到这些情况，我们可以安全地移除forceUpdate方法及相关调用，以简化代码并避免维护不必要的冗余代码。

### 5.2 异常处理优化

由于Upsert操作能够在一个SQL语句中处理插入和更新，与主键冲突相关的异常处理将减少，可以考虑：
- 简化与主键冲突相关的异常处理逻辑
- 移除不必要的重试机制

### 5.3 配置项检查

检查是否有针对覆盖模式的特殊配置项，如果有的话需要：
- 更新配置项说明文档
- 确保新旧配置的兼容性
- 在迁移指南中说明配置项的变化

### 5.4 测试代码更新

更新相关的单元测试和集成测试：
- 添加针对Upsert功能的测试用例
- 更新现有测试以验证新实现的正确性
- 性能测试对比优化前后的效果

### 5.5 文档更新

更新相关文档：
- 用户手册中添加Upsert功能说明
- API文档更新
- 迁移指南（如果需要）

## 6. 验证方案

为了确保优化方案的正确性和有效性，我们需要从以下几个维度进行验证：

### 6.1 技术可行性验证

1. **接口设计验证**：
   - 确认[SqlTemplate](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/sql/SqlTemplate.java#L26-L332)接口扩展的合理性
   - 验证各数据库特定实现的语法正确性
   - 确保与现有代码架构的兼容性

2. **代码实现验证**：
   - 验证[AbstractDatabaseConnector](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L59-L722)修改的正确性
   - 确认参数填充机制的兼容性
   - 验证异常处理逻辑的完整性

### 6.2 功能正确性验证

1. **单元测试**：
   - 为新增的`buildUpsertSql`方法编写单元测试
   - 验证各数据库特定实现的正确性
   - 测试边界条件和异常情况

2. **集成测试**：
   - 验证覆盖模式下的数据同步正确性
   - 测试不同数据库的Upsert功能
   - 验证与现有功能的兼容性

### 6.3 性能提升验证

1. **基准测试**：
   - 对比优化前后的数据同步性能
   - 测试不同数据量下的性能表现
   - 验证批量操作的效率提升

2. **资源消耗测试**：
   - 监控数据库服务器的负载情况
   - 测试网络往返时间的改善
   - 验证内存使用情况

### 6.4 兼容性验证

1. **向后兼容性**：
   - 确保未启用覆盖模式时功能不受影响
   - 验证现有配置的兼容性
   - 测试与其他模块的集成

2. **数据库兼容性**：
   - 验证各支持数据库的Upsert功能
   - 测试不同版本数据库的兼容性
   - 确认错误处理的一致性

## 7. 实施步骤

### 7.1 第一阶段：核心接口开发
1. 在 [SqlTemplate](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/sql/SqlTemplate.java#L26-L332) 接口中添加 `buildUpsertSql` 方法
2. 在 [ConnectorConstant](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/constant/ConnectorConstant.java#L12-L48) 中添加 `OPERTION_UPSERT` 常量

### 7.2 第二阶段：数据库特定实现
1. 实现 [MySQLTemplate](file:///E:/github/dbsyncer/dbsyncer-connector/dbsyncer-connector-mysql/src/main/java/org/dbsyncer/connector/mysql/MySQLTemplate.java#L22-L31) 的 `buildUpsertSql` 方法
2. 实现 [SqlServerTemplate](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/sql/impl/SqlServerTemplate.java#L22-L77) 的 `buildUpsertSql` 方法
3. 为其他支持的数据库实现相应的 Upsert 方法

### 7.3 第三阶段：连接器集成
1. 修改 [AbstractDatabaseConnector](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L59-L722) 的 [writer](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L208-L279) 方法，添加覆盖模式判断
2. 修改 [getTargetCommand](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L339-L377) 方法，生成 Upsert SQL

### 7.4 第四阶段：清理和重构
1. 移除forceUpdate方法及相关调用
2. 简化异常处理逻辑
3. 更新相关配置项和文档

### 7.5 第五阶段：测试和优化
1. 编写单元测试验证各数据库的 Upsert 功能
2. 进行性能测试，对比优化前后的性能提升
3. 完善异常处理和日志记录
4. 更新相关文档