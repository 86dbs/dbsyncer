# SQL Server DDL 同步功能实施文档

## 一、概述

在现有的 SQL Server 增量数据同步机制基础上，增加 DDL（数据定义语言）变更的捕获和同步功能。

**目标**：
- 捕获 SQL Server 表的 DDL 变更（ALTER TABLE）
- 捕获 SQL Server 特有的列重命名操作（sp_rename）
- 保持 DDL 和 DML 事件的时序一致性
- 支持断点续传（LSN 位置持久化）
- 确保 DDL 同步的完备性，防止因 DDL 同步不完备导致 DML 出现异常

**技术要点**：
- SQL Server CDC 的 `cdc.ddl_history` 表记录所有 DDL 变更
- DDL 和 DML 使用相同的 LSN 序列，可按 LSN 排序合并
- 需要按 LSN 升序处理，确保 DDL 在对应的 DML 之前执行
- SQL Server CDC 的限制：列结构变更后需要重新启用 CDC 才能捕获新列

## 二、核心实现架构

### 2.1 数据流程图

```
┌─────────────────────────────────────────────────────────────┐
│                    SQL Server Database                      │
│                  (CDC Enabled Tables)                       │
│                                                             │
│  数据变更 → CDC 捕获 → 写入 cdc.change_tables              │
│  DDL变更 → CDC 捕获 → 写入 cdc.ddl_history                 │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            │ SQL Server Agent 捕获变更
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    LsnPuller (单例)                        │
│                    Worker Thread                            │
│                                                             │
│  每 100ms 轮询:                                             │
│  1. 获取 maxLsn = max(dmlMaxLsn, ddlMaxLsn)                │
│  2. 比较 maxLsn > listener.getLastLsn()                    │
│  3. listener.pushStopLsn(maxLsn)                           │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            │ pushStopLsn()
                            ▼
┌─────────────────────────────────────────────────────────────┐
│              SqlServerListener.Buffer Queue                 │
│         BlockingQueue<Lsn> (容量: 256)                      │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            │ buffer.take() (阻塞)
                            ▼
┌─────────────────────────────────────────────────────────────┐
│            SqlServerListener.Worker Thread                   │
│                                                             │
│  1. 从队列取出 LSN (合并多个)                                │
│  2. 验证 LSN 有效性                                         │
│  3. pull(stopLsn)                                           │
│     - 计算 startLsn = fn_cdc_increment_lsn(lastLsn)        │
│     - 查询 DML 变更（提取 LSN）                              │
│     - 检查列名一致性（检测 sp_rename）                       │
│     - 查询 DDL 变更（cdc.ddl_history）                       │
│     - 合并并按 LSN 排序                                      │
│     - 按顺序解析和发送                                        │
│  4. 统一更新 lastLsn 并保存到 snapshot                       │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            │ sendChangedEvent()
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    AbstractListener                         │
│                                                             │
│  changeEvent() → watcher.changeEvent()                      │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 关键组件

| 组件                  | 职责                | 位置                                                       |
| ------------------- | ----------------- | -------------------------------------------------------- |
| `SqlServerListener` | 监听器主类，管理 CDC 生命周期 | `org.dbsyncer.connector.sqlserver.cdc.SqlServerListener` |
| `LsnPuller`         | 全局 LSN 轮询器，单例模式   | `org.dbsyncer.connector.sqlserver.cdc.LsnPuller`         |
| `Worker`            | 工作线程，处理变更数据       | `SqlServerListener.Worker`                               |
| `DDLEvent`          | DDL 事件封装          | `org.dbsyncer.connector.sqlserver.model.DDLEvent`        |
| `UnifiedChangeEvent`| 统一事件模型，合并 DDL 和 DML | `org.dbsyncer.connector.sqlserver.model.UnifiedChangeEvent` |

## 三、核心实现细节

### 3.1 DDL 相关 SQL 常量

```java
// DDL 相关 SQL
private static final String GET_DDL_CHANGES =
    "SELECT source_object_id, object_id, ddl_command, ddl_lsn, ddl_time " +
    "FROM cdc.ddl_history " +
    "WHERE ddl_lsn > ? AND ddl_lsn <= ? " +
    "ORDER BY ddl_lsn ASC";

// 获取 DDL 的最大 LSN（用于 LsnPuller 检测 DDL 变更）
private static final String GET_MAX_DDL_LSN =
    "SELECT MAX(ddl_lsn) FROM cdc.ddl_history";
```

**关键点**：
- `cdc.ddl_history` 表记录所有 DDL 变更，包括表、列、索引等
- 只查询 `ALTER TABLE` 相关的 DDL，过滤其他类型的 DDL
- DDL 和 DML 使用相同的 LSN 序列，可以按 LSN 排序合并

### 3.2 LSN 获取机制（getMaxLsn）

```java
/**
 * 获取最大 LSN（同时考虑 DML 和 DDL）
 * 
 * 问题说明：
 * - sys.fn_cdc_get_max_lsn() 只返回变更表（change tables）中的最大 LSN，不包括 DDL 的 LSN
 * - 当只有 DDL 发生而没有 DML 变更时，sys.fn_cdc_get_max_lsn() 不会变化
 * - 这导致 LsnPuller 无法检测到 DDL 变更，从而无法触发 pull() 方法
 * 
 * 解决方案：
 * - 同时查询 DML 和 DDL 的最大 LSN，取两者中的较大值
 * - 确保 DDL 变更也能被及时检测和处理
 */
public Lsn getMaxLsn() {
    // 获取 DML 的最大 LSN
    Lsn dmlMaxLsn = queryAndMap(GET_MAX_LSN, rs -> {
        byte[] bytes = rs.getBytes(1);
        return bytes != null ? new Lsn(bytes) : null;
    });

    // 获取 DDL 的最大 LSN（可能为 null，如果 cdc.ddl_history 表为空或不存在）
    Lsn ddlMaxLsn = queryAndMap(GET_MAX_DDL_LSN, rs -> {
        byte[] bytes = rs.getBytes(1);
        return bytes != null ? new Lsn(bytes) : null;
    });

    // 返回两者中的较大值
    if (dmlMaxLsn == null && ddlMaxLsn == null) {
        return null;
    }
    if (dmlMaxLsn == null) {
        return ddlMaxLsn;
    }
    if (ddlMaxLsn == null) {
        return dmlMaxLsn;
    }

    return dmlMaxLsn.compareTo(ddlMaxLsn) > 0 ? dmlMaxLsn : ddlMaxLsn;
}
```

**关键点**：
- **统一 LSN 管理**：DDL 和 DML 共享同一个 `lastLsn`，不单独维护 `lastDdlLsn`
- **断点续传**：通过 `snapshot.put(LSN_POSITION, lastLsn.toString())` 持久化位置
- **及时检测**：`getMaxLsn()` 同时考虑 DML 和 DDL，确保 DDL 变更能被及时检测

### 3.3 DDL 变更查询（pullDDLChanges）

```java
/**
 * 查询 DDL 变更（使用统一的 LSN）
 */
private List<DDLEvent> pullDDLChanges(Lsn startLsn, Lsn stopLsn) {
    return queryAndMapList(GET_DDL_CHANGES, statement -> {
        statement.setBytes(1, startLsn.getBinary());
        statement.setBytes(2, stopLsn.getBinary());
    }, rs -> {
        List<DDLEvent> events = new ArrayList<>();
        while (rs.next()) {
            String ddlCommand = rs.getString(3);
            Lsn ddlLsn = new Lsn(rs.getBytes(4));
            java.util.Date ddlTime = rs.getTimestamp(5);

            // 从 DDL 语句中解析表名（使用 JSQLParser）
            String tableName = extractTableNameFromDDL(ddlCommand);
            if (tableName == null) {
                logger.warn("无法从 DDL 命令中解析表名或 schema: {}", ddlCommand);
                continue;
            }

            logger.info("接收到 DDL : {}", ddlCommand);

            // 过滤 DDL 类型：只处理 ALTER TABLE
            if (!isTableAlterDDL(ddlCommand)) {
                continue;
            }

            // 检查 schema 和表名是否匹配
            if (filterTable.contains(tableName)) {
                events.add(new DDLEvent(tableName, ddlCommand, ddlLsn, ddlTime));
            }
        }
        return events;
    });
}
```

**关键点**：
- **表名解析**：使用 `extractTableNameFromDDL()` 从 DDL 语句中解析表名，而不是通过 `object_id` 查询
- **Schema 验证**：`extractTableNameFromDDL()` 内部会验证 schema，只返回匹配的表名
- **DDL 过滤**：只处理 `ALTER TABLE` 相关的 DDL，过滤其他类型（如 CREATE INDEX）

### 3.4 事件合并与排序（mergeAndSortEvents）

```java
/**
 * 合并并排序 DDL 和 DML 事件
 */
private List<UnifiedChangeEvent> mergeAndSortEvents(
        List<DDLEvent> ddlEvents,
        List<CDCEvent> dmlEvents) {

    List<UnifiedChangeEvent> unifiedEvents = new ArrayList<>();

    // 转换 DDL 事件
    ddlEvents.forEach(ddlEvent -> {
        UnifiedChangeEvent event = new UnifiedChangeEvent();
        event.setLsn(ddlEvent.getDdlLsn());
        event.setEventType("DDL");
        event.setTableName(ddlEvent.getTableName());
        event.setDdlCommand(ddlEvent.getDdlCommand());
        unifiedEvents.add(event);
    });

    // 转换 DML 事件
    dmlEvents.forEach(dmlEvent -> {
        UnifiedChangeEvent event = new UnifiedChangeEvent();
        event.setLsn(dmlEvent.getLsn());
        event.setEventType("DML");
        event.setTableName(dmlEvent.getTableName());
        event.setCdcEvent(dmlEvent);
        unifiedEvents.add(event);
    });

    // 按 LSN 排序（确保 DDL 在对应的 DML 之前执行）
    unifiedEvents.sort((e1, e2) -> {
        Lsn lsn1 = e1.getLsn();
        Lsn lsn2 = e2.getLsn();
        if (lsn1 == null && lsn2 == null) return 0;
        if (lsn1 == null) return -1;
        if (lsn2 == null) return 1;
        return lsn1.compareTo(lsn2);
    });

    return unifiedEvents;
}
```

**关键点**：
- **时序保证**：按 LSN 升序排序，确保 DDL 在对应的 DML 之前执行
- **统一事件模型**：使用 `UnifiedChangeEvent` 统一封装 DDL 和 DML 事件

### 3.5 DDL 事件发送（trySendDDLEvent）

```java
/**
 * 发送 DDL 事件
 */
private void trySendDDLEvent(String tableName, String ddlCommand, Lsn position) {
    DDLChangedEvent ddlEvent = new DDLChangedEvent(
            tableName,
            ConnectorConstant.OPERTION_ALTER,
            ddlCommand,
            null,
            position
    );

    while (connected) {
        try {
            sendChangedEvent(ddlEvent);
            
            // 如果 DDL 操作会影响列结构（增删改列），需要重新启用表的 CDC
            if (isColumnStructureChangedDDL(ddlCommand)) {
                reEnableTableCDC(tableName);
            }
            
            break;
        } catch (QueueOverflowException ex) {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
            } catch (InterruptedException exe) {
                logger.error(exe.getMessage(), exe);
            }
        }
    }
}
```

**关键点**：
- **阻塞重试**：如果事件队列已满，等待 1 毫秒后重试
- **CDC 重新启用**：如果 DDL 操作会影响列结构，需要重新启用表的 CDC

## 四、SQL Server 特有的列重命名处理

### 4.1 sp_rename 的限制

SQL Server 使用 `sp_rename` 存储过程来重命名列，但该操作**不会出现在 `cdc.ddl_history` 中**，因为 `sp_rename` 不是标准的 ALTER TABLE 语句。

**问题**：
- `sp_rename` 重命名列后，CDC 捕获的列名仍然是旧列名
- 如果直接使用旧列名处理 DML 事件，会导致列名不匹配错误

### 4.2 列重命名检测机制

在每次查询 DML 变更前，会检查表的列名是否与 CDC 捕获的列名一致：

```java
private void pull(Lsn stopLsn) {
    Lsn startLsn = queryAndMap(GET_INCREMENT_LSN, statement -> statement.setBytes(1, lastLsn.getBinary()), rs -> Lsn.valueOf(rs.getBytes(1)));

    // 查询 DML 变更（提取 LSN 用于排序）
    List<CDCEvent> dmlEvents = new ArrayList<>();
    changeTables.forEach(changeTable -> {
        // 检查列名是否一致（用于检测 sp_rename 重命名列的情况）
        checkAndReEnableCDCIfNeeded(changeTable);
        
        // ... 查询 DML 变更 ...
    });
    
    // ... 查询 DDL 变更 ...
}
```

### 4.3 列名一致性检查（checkAndReEnableCDCIfNeeded）

```java
/**
 * 检查表的列名是否与 CDC 捕获的列名一致，如果不一致则重新启用 CDC
 * 
 * 用于检测 sp_rename 重命名列的情况（sp_rename 不会出现在 cdc.ddl_history 中）
 */
private void checkAndReEnableCDCIfNeeded(SqlServerChangeTable changeTable) {
    try {
        String tableName = changeTable.getTableName();
        String capturedColumns = changeTable.getCapturedColumns();
        
        // 获取当前表的所有列名
        List<String> currentColumns = queryAndMapList(
                String.format(GET_TABLE_COLUMNS, schema, tableName),
                rs -> {
                    List<String> colList = new ArrayList<>();
                    while (rs.next()) {
                        colList.add(rs.getString(1));
                    }
                    return colList;
                }
        );
        
        // 解析 CDC 捕获的列名（逗号分隔）
        List<String> capturedColumnList = Arrays.stream(capturedColumns.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(java.util.stream.Collectors.toList());
        
        // 比较列数和列名
        if (currentColumns.size() != capturedColumnList.size()) {
            // 列数不同，说明可能有列增删操作
            logger.warn("表 [{}] 的列数与 CDC 捕获的列数不一致（当前: {}, CDC: {}），重新启用 CDC", 
                    tableName, currentColumns.size(), capturedColumnList.size());
            reEnableTableCDC(tableName);
            return;
        }
        
        // 检查列名是否完全一致（用于检测 sp_rename 重命名列）
        Set<String> currentColumnSet = new HashSet<>(currentColumns);
        Set<String> capturedColumnSet = new HashSet<>(capturedColumnList);
        
        if (!currentColumnSet.equals(capturedColumnSet)) {
            // 列名不一致，说明可能有列重命名操作（sp_rename）
            logger.warn("表 [{}] 的列名与 CDC 捕获的列名不一致，检测到可能的列重命名操作（sp_rename）", tableName);
            
            // 使用基于 hash 的匹配算法检测并发送列重命名 DDL 事件
            detectAndSendRenameDDLEventsWithHash(tableName, changeTable, currentColumns, capturedColumnList);
            
            // 重新启用 CDC
            reEnableTableCDC(tableName);
        }
        
    } catch (Exception e) {
        logger.error("检查表 [{}] 的列名一致性时出错: {}", changeTable.getTableName(), e.getMessage(), e);
    }
}
```

**关键点**：
- **列数检查**：如果列数不同，说明有列增删操作，需要重新启用 CDC
- **列名检查**：如果列名不一致，说明可能有列重命名操作（sp_rename）
- **重命名检测**：使用基于 hash 的匹配算法检测并发送列重命名 DDL 事件

### 4.4 列重命名 DDL 事件生成

当检测到列重命名时，会生成一个 DDL 事件发送到目标端：

```java
/**
 * 构造列重命名的 DDL 语句
 * 
 * 使用 MySQL 的 CHANGE COLUMN 语法，因为 DDL 解析器能理解这个语法
 * DDL 解析器会自动转换为目标数据库的语法（如 SQL Server 的 sp_rename）
 */
private String buildRenameColumnDDL(String tableName, String oldColumnName, String newColumnName, ColumnInfo columnInfo) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("ALTER TABLE ").append(tableName)
       .append(" CHANGE COLUMN ").append(oldColumnName)
       .append(" ").append(newColumnName);
    
    // 添加类型信息
    String typeStr = columnInfo.dataType;
    if (columnInfo.maxLength != null) {
        typeStr += "(" + columnInfo.maxLength + ")";
    } else if (columnInfo.precision != null) {
        typeStr += "(" + columnInfo.precision;
        if (columnInfo.scale != null) {
            typeStr += "," + columnInfo.scale;
        }
        typeStr += ")";
    }
    ddl.append(" ").append(typeStr);
    
    // 添加 NOT NULL 约束
    if (!columnInfo.nullable) {
        ddl.append(" NOT NULL");
    }
    
    // 添加 DEFAULT 值
    if (columnInfo.defaultValue != null && !columnInfo.defaultValue.trim().isEmpty()) {
        ddl.append(" DEFAULT ").append(columnInfo.defaultValue);
    }
    
    return ddl.toString();
}
```

**关键点**：
- **DDL 语法**：使用 MySQL 的 `CHANGE COLUMN` 语法，DDL 解析器会自动转换为目标数据库的语法
- **类型信息**：包含完整的列类型、长度、精度、是否可空、默认值等信息

## 五、DDL 同步完备性保证

### 5.1 SQL Server CDC 的限制

SQL Server CDC 有一个重要限制：**当表已启用 CDC 后，列结构的变更不会自动反映到 CDC 捕获列表中**。

**影响**：
- 如果添加了新列，CDC 不会自动捕获新列的数据
- 如果删除了列，CDC 仍然会尝试捕获已删除的列，导致错误
- 如果修改了列类型，CDC 仍然使用旧的列类型，可能导致数据不一致

### 5.2 列结构变更检测（isColumnStructureChangedDDL）

```java
/**
 * 判断 DDL 操作是否会改变列结构（需要重新启用 CDC）
 * 
 * 支持的 DDL 操作类型：
 * - ALTER TABLE ... ADD ... - 添加列（SQL Server 语法：ADD column_name，不需要 COLUMN 关键字）
 * - ALTER TABLE ... DROP COLUMN ... - 删除列
 * - ALTER TABLE ... ALTER COLUMN ... - 修改列（类型、约束等）
 */
private boolean isColumnStructureChangedDDL(String ddlCommand) {
    if (ddlCommand == null) {
        return false;
    }
    String upperCommand = ddlCommand.toUpperCase().trim();
    
    // 必须是 ALTER TABLE 操作
    if (!upperCommand.startsWith("ALTER TABLE")) {
        return false;
    }
    
    // 检查是否包含会影响列结构的操作
    return upperCommand.contains(" ADD ") ||           // 添加列
           upperCommand.contains("DROP COLUMN") ||     // 删除列
           upperCommand.contains("ALTER COLUMN");     // 修改列
}
```

### 5.3 CDC 重新启用机制（reEnableTableCDC）

当检测到列结构变更时，会重新启用表的 CDC：

```java
/**
 * 重新启用表的 CDC（用于在列结构变更后更新捕获列列表）
 * 
 * 适用场景：
 * - ADD COLUMN: 需要捕获新列
 * - DROP COLUMN: 需要移除已删除的列
 * - RENAME COLUMN: 需要更新列名（sp_rename）
 * - ALTER COLUMN: 需要更新列类型/约束信息
 */
private void reEnableTableCDC(String tableName) {
    try {
        // 1. 获取表的所有列名（包括新列）
        List<String> columns = queryAndMapList(
                String.format(GET_TABLE_COLUMNS, schema, tableName),
                rs -> {
                    List<String> colList = new ArrayList<>();
                    while (rs.next()) {
                        colList.add(rs.getString(1));
                    }
                    return colList;
                }
        );

        // 2. 禁用表的 CDC（所有 capture_instance）
        execute(String.format(DISABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, tableName), schema));
        logger.info("已禁用表 [{}] 的 CDC", tableName);

        // 等待一下，确保禁用操作完成
        TimeUnit.MILLISECONDS.sleep(500);

        // 3. 重新启用表的 CDC，指定所有列（包括新列）
        String columnList = String.join(", ", columns);
        String enableSql = String.format(
                "EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'%s', @role_name = NULL, @supports_net_changes = 0, @captured_column_list = N'%s'",
                schema, tableName, columnList
        );
        execute(enableSql);
        logger.info("已重新启用表 [{}] 的 CDC，捕获列: {}", tableName, columnList);

        // 4. 刷新 changeTables 缓存
        readChangeTables();
        logger.info("已刷新 changeTables 缓存");

    } catch (Exception e) {
        logger.error("重新启用表 [{}] 的 CDC 失败: {}", tableName, e.getMessage(), e);
    }
}
```

**关键点**：
- **禁用再启用**：必须先禁用表的 CDC，然后重新启用，才能更新捕获列列表
- **指定列列表**：使用 `@captured_column_list` 参数指定要捕获的所有列
- **缓存刷新**：重新启用后，需要刷新 `changeTables` 缓存，确保后续查询使用新的列列表

### 5.4 DDL 同步完备性检查清单

| DDL 操作类型 | 是否支持 | 处理方式 | 完备性保证 |
| ---------- | ------ | ------- | --------- |
| ALTER TABLE ... ADD COLUMN | ✅ | 重新启用 CDC，捕获新列 | ✅ 完备 |
| ALTER TABLE ... DROP COLUMN | ✅ | 重新启用 CDC，移除已删除列 | ✅ 完备 |
| ALTER TABLE ... ALTER COLUMN | ✅ | 重新启用 CDC，更新列类型 | ✅ 完备 |
| sp_rename 重命名列 | ✅ | 检测列名变化，发送 DDL 事件，重新启用 CDC | ✅ 完备 |
| ALTER TABLE ... ADD CONSTRAINT | ⚠️ | 不重新启用 CDC（不影响列结构） | ✅ 完备 |
| CREATE INDEX / DROP INDEX | ❌ | 过滤（不处理索引相关 DDL） | ✅ 完备 |

**完备性保证机制**：
1. **DDL 事件捕获**：通过 `cdc.ddl_history` 捕获所有 ALTER TABLE 操作
2. **列重命名检测**：通过列名比较检测 `sp_rename` 操作
3. **CDC 重新启用**：列结构变更后自动重新启用 CDC，确保后续 DML 能正确捕获
4. **时序保证**：DDL 和 DML 按 LSN 排序，确保 DDL 在对应的 DML 之前执行

## 六、关键注意事项

### 6.1 Schema 处理

- 从 DDL 语句中解析表名时，会验证 schema 是否匹配
- 使用 `extractTableNameFromDDL()` 方法解析表名，内部会检查 schema
- 只有 schema 和表名都匹配的 DDL 才会被处理

### 6.2 DDL 类型过滤

- 只处理 `ALTER TABLE` 相关的 DDL
- 过滤 `CREATE INDEX`、`ALTER INDEX`、`DROP INDEX` 等其他 DDL
- 过滤非表级别的 DDL（如数据库级别、视图级别等）

### 6.3 表删除场景

- 如果表被删除，`cdc.ddl_history` 中可能仍然有该表的 DDL 记录
- 如果无法从 DDL 语句中解析表名，会记录警告日志并跳过该 DDL 事件

### 6.4 时序保证

- 必须按 LSN 升序排序，确保 DDL 在对应的 DML 之前处理
- 排序使用 `Lsn.compareTo()` 方法
- 只有最后一个事件会携带 `stopLsn` 作为位置标记

### 6.5 性能优化

- 提前过滤非 ALTER TABLE 的 DDL
- 批量查询 DDL 变更
- 列重命名检测使用基于 hash 的匹配算法，提高性能

### 6.6 异常处理

- **队列溢出**：阻塞重试，确保事件不丢失
- **CDC 重新启用失败**：记录错误日志，但不影响后续处理
- **列重命名检测失败**：回退到位置匹配算法

## 七、与增量同步机制的集成

### 7.1 初始化流程

在 `start()` 方法中，DDL 同步功能会自动启用：

```java
@Override
public void start() {
    // ... 连接数据库、启用 CDC 等 ...
    readLastLsn();  // 读取最后处理的 LSN（统一管理 DDL 和 DML）
    worker = new Worker();  // 启动工作线程
    worker.start();
    LsnPuller.addExtractor(metaId, this);  // 注册到 LsnPuller
}
```

### 7.2 LSN 轮询机制

`LsnPuller` 会定期调用 `getMaxLsn()` 方法，该方法会同时考虑 DML 和 DDL 的最大 LSN：

```java
// LsnPuller.Worker.run()
Lsn maxLsn = null;
for (SqlServerListener listener : map.values()) {
    maxLsn = listener.getMaxLsn();  // 同时考虑 DML 和 DDL
    if (null != maxLsn && maxLsn.isAvailable() && maxLsn.compareTo(listener.getLastLsn()) > 0) {
        listener.pushStopLsn(maxLsn);
    }
}
```

### 7.3 事件处理流程

```java
final class Worker extends Thread {
    @Override
    public void run() {
        while (!isInterrupted() && connected) {
            try {
                Lsn stopLsn = buffer.take();
                // ... 合并多个 LSN ...
                
                pull(stopLsn);  // 查询并处理 DDL 和 DML 变更
                
                // LSN 已在 parseUnifiedEvents() 中统一更新
            } catch (InterruptedException e) {
                break;
            } catch (Exception e) {
                // ... 异常处理 ...
            }
        }
    }
}
```

## 八、相关代码文件

| 文件路径                         | 说明       |
| ---------------------------- | -------- |
| `SqlServerListener.java`     | 监听器主类，包含 DDL 同步逻辑 |
| `LsnPuller.java`             | LSN 轮询器，调用 getMaxLsn() |
| `Lsn.java`                   | LSN 值对象  |
| `DDLEvent.java`              | DDL 事件模型 |
| `UnifiedChangeEvent.java`    | 统一事件模型 |
| `CDCEvent.java`              | CDC 事件封装，包含 LSN 信息 |

## 九、参考文档

- [SQL Server CDC 官方文档](https://learn.microsoft.com/zh-cn/sql/relational-databases/track-changes/about-change-data-capture-sql-server)
- [cdc.ddl_history 表文档](https://learn.microsoft.com/zh-cn/sql/relational-databases/system-tables/cdc-ddl-history-transact-sql)
- [fn_cdc_get_all_changes 函数文档](https://learn.microsoft.com/zh-cn/previous-versions/sql/sql-server-2008/bb510627(v=sql.100))
- [sp_rename 存储过程文档](https://learn.microsoft.com/zh-cn/sql/relational-databases/system-stored-procedures/sp-rename-transact-sql)
