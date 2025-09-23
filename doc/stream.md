# DBSyncer 流式结果集全量同步实施方案

## 1. 背景与目标

### 1.1 当前问题
当前DBSyncer全量同步采用分页查询方式，存在以下问题：
- 每次分页查询都需要执行完整的SQL语句，对数据库造成较大压力
- OFFSET值较大时，查询性能下降明显
- 无法充分利用数据库连接资源
- 大数据量同步时内存占用过高

### 1.2 目标
基于最小影响原则，使用JDBC流式结果集替代分页查询方式，同时保持断点续传功能，提升全量同步性能和效率。
核心目标：实现真正的流式处理，避免内存溢出，同时保持系统稳定性。

## 2. 流式结果集方案设计

### 2.1 核心思路
- 使用JDBC流式结果集（Streaming ResultSet）实现真正的流式处理
- 通过游标方式实现流式模式下的断点续传能力
- 默认使用流式处理模式
- 区分流式处理和分页处理的SQL生成逻辑

### 2.2 技术实现要点

#### 2.2.1 真正的流式读取实现
```java
// 设置Statement属性以启用流式结果集
Statement stmt = connection.createStatement(
    ResultSet.TYPE_FORWARD_ONLY,  // 只能向前滚动（流式处理推荐）
    ResultSet.CONCUR_READ_ONLY    // 只读模式（流式处理推荐）
);
// 由具体连接器决定fetchSize的设置方式
stmt.setFetchSize(fetchSizeFromConnector); 

// 使用Stream进行真正的流式处理
try (Stream<Map<String, Object>> stream = databaseTemplate.queryForStream(sql, rowMapper)) {
    stream.forEach(row -> {
        // 逐行处理数据，避免内存累积
        processRow(row);
    });
}
```

#### 2.2.2 流式模式下的断点续传机制
- 使用游标（Cursor）方式记录处理进度
- 基于主键值实现精确的断点定位
- 支持任务中断后的精确恢复

## 3. 实施方案

使用 JDBC 流式结果集（Streaming ResultSet）实现真正的流式处理，通过分批处理避免内存溢出，同时支持基于游标的断点续传机制。

### 3.1 核心实现要点

1. 在 [AbstractDatabaseConnector](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L59-L722) 类中添加抽象方法 `getStreamingFetchSize()` 供各数据库连接器实现
2. 修改 [AbstractDatabaseConnector.getSourceCommand()](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L277-L309) 方法，区分流式处理和分页处理的SQL生成
3. 各数据库连接器实现 `getStreamingFetchSize()` 方法返回各自适合的 fetchSize 值
4. 重新设计 [ParserComponentImpl.execute](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/impl/ParserComponentImpl.java#L116-L148) 方法，实现真正的流式处理逻辑
5. 在 [ParamKeyEnum](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/enums/ParamKeyEnum.java#L12-L59) 中添加流式处理相关参数配置

### 3.2 DatabaseTemplate 类说明

[DatabaseTemplate](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/DatabaseTemplate.java#L83-L1698) 类已经实现了 `fetchSize` 属性及相关 getter/setter 方法，并在 [applyStatementSettings](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/DatabaseTemplate.java#L1431-L1442) 方法中应用 fetchSize 设置，无需额外修改。

DatabaseTemplate 中的 [queryForStream](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/DatabaseTemplate.java#L764-L776) 方法已经实现了基于 ResultSet 的流式处理，返回一个 Stream 对象，可以逐行处理数据而不会将所有数据加载到内存中。

### 3.3 AbstractDatabaseConnector 类修改

#### 3.3.1 添加抽象方法

在 [AbstractDatabaseConnector](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L59-L722) 类中添加抽象方法：

```java
/**
 * 获取流式处理的 fetchSize 值
 * 
 * @param context 读取上下文
 * @return fetchSize 值
 */
public abstract Integer getStreamingFetchSize(ReaderContext context);
```

#### 3.3.2 修改getSourceCommand方法

在 [getSourceCommand](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L277-L309) 方法中，需要区分流式处理和分页处理的SQL生成：

```java
// 现有代码保持不变，只需要修改SQL生成部分
Map<String, String> map = new HashMap<>();

// 新增：生成纯查询SQL（用于流式处理）
SqlBuilderQuery queryBuilder = new SqlBuilderQuery();
String streamingQuerySql = queryBuilder.buildQuerySql(config);
map.put(ConnectorConstant.OPERTION_QUERY, streamingQuerySql);

// 新增：生成分页查询SQL（用于分页模式）
String paginationQuerySql = buildPaginationQuerySql(config);
map.put(ConnectorConstant.OPERTION_QUERY_WITH_PAGINATION, paginationQuerySql);

// 现有代码保持不变
if (enableCursor()) {
    buildSql(map, SqlBuilderEnum.QUERY_CURSOR, config);
}
map.put(ConnectorConstant.OPERTION_QUERY_COUNT, getQueryCountSql(...));
```

**streamingQuerySql 生成说明：**

`SqlBuilderQuery.buildQuerySql()` 方法会生成不带分页的纯查询SQL，格式如下：

```sql
-- 示例：SELECT "ID","NAME","AGE" FROM "SCHEMA"."USER" WHERE "AGE" > 18
SELECT [字段列表] FROM [架构].[表名] [WHERE条件]
```

具体生成逻辑：
1. **字段列表**：根据配置的同步字段生成，支持字段别名
2. **表名**：包含架构名和表名，使用数据库特定的引号
3. **WHERE条件**：包含用户配置的过滤条件（如年龄>18等）
4. **无分页**：不包含LIMIT、OFFSET等分页子句

**SQL对比示例：**

```sql
-- 流式查询SQL（streamingQuerySql）
SELECT "ID","NAME","AGE" FROM "SCHEMA"."USER" WHERE "AGE" > 18

-- 分页查询SQL（paginationQuerySql）
SELECT "ID","NAME","AGE" FROM "SCHEMA"."USER" WHERE "AGE" > 18 LIMIT 1000 OFFSET 0

-- 游标查询SQL（cursorQuerySql）
SELECT "ID","NAME","AGE" FROM "SCHEMA"."USER" WHERE "AGE" > 18 AND "ID" > ? ORDER BY "ID" LIMIT 1000
```

**关键区别：**
- **流式查询**：一次性获取全部数据，通过JDBC的fetchSize控制内存使用
- **分页查询**：分批获取数据，每次查询指定数量的记录
- **游标查询**：基于主键值进行分页，避免OFFSET性能问题


#### 3.3.3 修改reader方法

在 [reader](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L204-L228) 方法中，需要添加流式处理逻辑：

```java
@Override
public Result reader(DatabaseConnectorInstance connectorInstance, ReaderContext context) {
    // 流式处理逻辑 - 返回空Result，实际处理在ParserComponentImpl中
    final Result result = new Result();
    return result;
}

```


### 3.4 ParserComponentImpl 类修改

在 [ParserComponentImpl.execute](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/impl/ParserComponentImpl.java#L116-L148) 方法中，基于原有逻辑进行最小化修改：

```java
@Override
public void execute(Task task, Mapping mapping, TableGroup tableGroup, Executor executor) {
    // 现有初始化代码保持不变
    // ... 现有代码 ...
    
    // 使用流式处理模式（缺省行为）
    executeWithStreaming(task, context, tableGroup, executor, primaryKeys);
}

// 分页处理模式
private void executeWithPagination(Task task, FullPluginContext context, TableGroup tableGroup, 
                                  Executor executor, List<String> primaryKeys) {
    for (;;) {
        if (!task.isRunning()) {
            logger.warn("任务被中止:{}", metaId);
            break;
        }
        
        context.setArgs(new ArrayList<>());
        context.setCursors(task.getCursors());
        context.setPageIndex(task.getPageIndex());
        Result reader = connectorFactory.reader(context);
        List<Map> source = reader.getSuccessData();
        
        if (CollectionUtils.isEmpty(source)) {
            logger.info("完成全量同步任务:{}, [{}] >> [{}]", metaId, sTableName, tTableName);
            break;
        }
        
        // 抽离：数据处理逻辑
        processDataBatch(source, task, context, tableGroup, executor, primaryKeys);
        
        // 判断尾页
        if (source.size() < context.getPageSize()) {
            logger.info("完成全量:{}, [{}] >> [{}]", metaId, sTableName, tTableName);
            break;
        }
    }
}

// 流式处理模式
private void executeWithStreaming(Task task, FullPluginContext context, TableGroup tableGroup, 
                                 Executor executor, List<String> primaryKeys) {
    String querySql = context.getCommand().get(ConnectorConstant.OPERTION_QUERY);
    context.getSourceConnectorInstance().execute(databaseTemplate -> {
        databaseTemplate.setFetchSize(getStreamingFetchSize(context));
        try (Stream<Map<String, Object>> stream = databaseTemplate.queryForStream(querySql, rowMapper)) {
            List<Map<String, Object>> batch = new ArrayList<>();
            Iterator<Map<String, Object>> iterator = stream.iterator();
            
            while (iterator.hasNext()) {
                if (!task.isRunning()) {
                    logger.warn("任务被中止:{}", metaId);
                    break;
                }
                
                batch.add(iterator.next());
                
                // 达到批次大小时处理数据
                if (batch.size() >= context.getBatchSize()) {
                    processDataBatch(batch, task, context, tableGroup, executor, primaryKeys);
                    batch = new ArrayList<>();
                }
            }
            
            // 处理最后一批数据
            if (!batch.isEmpty()) {
                processDataBatch(batch, task, context, tableGroup, executor, primaryKeys);
            }
        }
        return null;
    });
}

// 抽离：数据处理逻辑（从现有代码中提取）
private void processDataBatch(List<Map> source, Task task, FullPluginContext context, 
                             TableGroup tableGroup, Executor executor, List<String> primaryKeys) {
    // 1、映射字段
    List<Map> target = picker.pickTargetData(source);
    
    // 2、参数转换
    ConvertUtil.convert(group.getConvert(), target);
    
    // 3、插件转换
    context.setSourceList(source);
    context.setTargetList(target);
    pluginFactory.process(group.getPlugin(), context, ProcessEnum.CONVERT);
    
    // 4、写入目标源
    Result result = writeBatch(context, executor);
    
    // 5、更新结果和断点
    task.setPageIndex(task.getPageIndex() + 1);
    task.setCursors(PrimaryKeyUtil.getLastCursors(source, primaryKeys));
    result.setTableGroupId(tableGroup.getId());
    result.setTargetTableGroupName(tTableName);
    flush(task, result);
    
    // 6、同步完成后通知插件做后置处理
    pluginFactory.process(group.getPlugin(), context, ProcessEnum.AFTER);
}
```

### 3.5 各数据库连接器实现

各数据库连接器需要实现 `getStreamingFetchSize` 方法：

```java
// MySQL连接器
@Override
public Integer getStreamingFetchSize(ReaderContext context) {
    return Integer.MIN_VALUE; // MySQL流式处理特殊值
}

// PostgreSQL/Oracle连接器
@Override
public Integer getStreamingFetchSize(ReaderContext context) {
    return context.getPageSize(); // 使用页面大小作为fetchSize
}

// SQL Server连接器
@Override
public Integer getStreamingFetchSize(ReaderContext context) {
    return context.getPageSize(); // 使用页面大小作为fetchSize
}
```

#### 3.5.1 SQL Server 游标分页支持

为了支持SQL Server的高效游标分页，需要在 `SqlServerConnector` 中添加以下方法：

```java
// 1. 启用游标支持
@Override
public boolean enableCursor() {
    return true;
}

// 2. 实现游标分页SQL生成
@Override
public String getPageCursorSql(PageSql config) {
    // 不支持游标查询
    if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
        logger.debug("不支持游标查询，主键包含非数字类型");
        return StringUtil.EMPTY;
    }

    // SQL Server 游标分页：SELECT TOP ? * FROM [table] WHERE [id] > ? ORDER BY [id]
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT TOP ? * FROM (");
    sql.append(config.getQuerySql());
    sql.append(") S");
    
    boolean skipFirst = false;
    // 没有过滤条件
    if (StringUtil.isBlank(config.getQueryFilter())) {
        skipFirst = true;
        sql.append(" WHERE ");
    }
    
    final String quotation = buildSqlWithQuotation(); // 返回 "["
    final List<String> primaryKeys = config.getPrimaryKeys();
    PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, " AND ", " > ? ", skipFirst);
    appendOrderByPk(config, sql);
    
    return sql.toString();
}

// 3. 实现游标分页参数构建
@Override
public Object[] getPageCursorArgs(ReaderContext context) {
    int pageSize = context.getPageSize();
    Object[] cursors = context.getCursors();
    if (null == cursors) {
        return new Object[]{pageSize}; // 只有 TOP 参数
    }
    int cursorsLen = cursors.length;
    Object[] newCursors = new Object[cursorsLen + 1];
    newCursors[0] = pageSize; // TOP 参数在前
    System.arraycopy(cursors, 0, newCursors, 1, cursorsLen); // 游标参数在后
    return newCursors;
}
```

**SQL Server 游标分页示例：**

```sql
-- 基础查询
SELECT * FROM [SCHEMA].[USER] WHERE [AGE] > 18

-- 游标分页SQL（断点续传）
SELECT TOP 1000 * FROM (
    SELECT * FROM [SCHEMA].[USER] WHERE [AGE] > 18
) S WHERE [ID] > ? ORDER BY [ID]

-- 参数：[1000, 1000] (pageSize, cursorValue)
```

### 3.6 参数配置

在 [ParamKeyEnum](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/enums/ParamKeyEnum.java#L12-L59) 中添加流式处理相关参数：

```java
// 流式处理相关参数
STREAMING_FETCH_SIZE("stream.fetchSize", "流式处理批次大小", "int", "流式处理时的fetchSize值，-1表示使用默认值");
```

### 3.7 使用方法

在映射配置中添加参数来控制是否使用流式处理：

1. 进入映射配置页面
2. 在"扩展参数"区域点击"添加参数"
3. 选择"禁用流式处理"参数
4. 设置参数值：
    - "true"：禁用流式处理，使用分页查询方式
    - "false"或留空：启用流式处理，使用流式结果集方式

### 3.8 注意事项

1. **内存管理**：流式处理通过分批处理避免内存溢出，但仍需合理设置批次大小
2. **断点续传**：流式模式下基于游标实现断点续传，需要主键支持
3. **数据库兼容性**：不同数据库对流式处理的支持方式不同，需要根据具体数据库选择合适的fetchSize值
4. **性能优化**：流式处理模式下SQL只执行一次，提高了执行效率
5. **错误处理**：流式处理中的错误恢复机制需要特别关注

## 4. 断点续传和错误处理

### 4.1 断点续传实现

流式处理模式下的断点续传通过修改查询SQL实现：

```java
// 构建支持断点续传的查询SQL
private String buildStreamingQuerySql(FullPluginContext context, Object[] lastCursors, List<String> primaryKeys) {
    String baseSql = context.getCommand().get(ConnectorConstant.OPERTION_QUERY);
    
    // 如果没有断点，直接返回基础SQL
    if (lastCursors == null || lastCursors.length == 0) {
        return baseSql;
    }
    
    // 构建带游标的查询SQL
    StringBuilder sql = new StringBuilder(baseSql);
    sql.append(baseSql.contains("WHERE") ? " AND " : " WHERE ");
    
    // 构建游标条件：WHERE (pk1 > ? OR (pk1 = ? AND pk2 > ?)) ORDER BY pk1, pk2
    buildCursorCondition(sql, primaryKeys, lastCursors);
    buildOrderByClause(sql, primaryKeys);
    
    return sql.toString();
}
```

### 4.2 错误处理机制

在流式处理中添加重试机制：

```java
// 在流式查询外层添加错误处理
try {
    // 流式查询逻辑
    context.getSourceConnectorInstance().execute(databaseTemplate -> {
        // ... 流式处理逻辑 ...
    });
} catch (Exception e) {
    logger.error("流式处理执行失败: {}", e.getMessage());
    // 根据业务需求决定是否重试或抛出异常
    throw e;
}
```

## 5. 实施步骤

### 5.1 第一阶段：基础框架搭建
1. 在 [AbstractDatabaseConnector](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L59-L722) 中添加 `getStreamingFetchSize()` 抽象方法
2. 修改 [getSourceCommand()](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java#L277-L309) 方法，区分流式处理和分页处理的SQL生成
3. 在 [ParamKeyEnum](file:///E:/github/dbsyncer/dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/enums/ParamKeyEnum.java#L12-L59) 中添加流式处理相关参数

### 5.2 第二阶段：各数据库连接器实现
1. 在各数据库连接器中实现 `getStreamingFetchSize()` 方法
2. 为SQL Server连接器添加游标分页支持（`enableCursor()`, `getPageCursorSql()`, `getPageCursorArgs()`）
3. 测试各数据库的流式处理兼容性
4. 优化各数据库的fetchSize设置

### 5.3 第三阶段：核心处理逻辑重构
1. 重新设计 [ParserComponentImpl.execute](file:///E:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/impl/ParserComponentImpl.java#L116-L148) 方法
2. 实现真正的流式处理逻辑
3. 实现基于游标的断点续传机制

### 5.4 第四阶段：测试和优化
1. 进行大数据量同步测试
2. 验证内存使用情况
3. 测试断点续传功能
4. 性能调优和错误处理完善

## 6. 总结

本方案通过重新设计流式处理逻辑，实现了真正的流式数据处理，避免了内存溢出问题，同时保持了断点续传功能。主要改进包括：

1. **真正的流式处理**：使用Stream API逐行处理数据，避免将所有数据加载到内存
2. **SQL生成优化**：区分流式处理和分页处理的SQL生成逻辑
3. **断点续传改进**：基于游标实现流式模式下的断点续传
4. **SQL Server游标分页支持**：通过`SELECT TOP ? * FROM (...) WHERE [id] > ? ORDER BY [id]`实现高效分页
5. **错误处理增强**：添加重试机制和错误恢复功能
6. **向后兼容**：通过参数控制支持原有分页查询方式

该方案在保持系统稳定性的同时，显著提升了大数据量同步的性能和可靠性，特别是为SQL Server提供了高效的游标分页支持。