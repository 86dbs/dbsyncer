# SQL Server Change Tracking (CT) 同步方案

## 一、概述

基于 SQL Server Change Tracking (CT) 实现 DML 和 DDL 同步，解决 CDC 方案的局限性。

### 1.1 Change Tracking vs CDC 对比

| 特性 | CDC (Change Data Capture) | Change Tracking (CT) |
|------|---------------------------|---------------------|
| **依赖** | 需要 SQL Server Agent | 不需要 Agent，内置于数据库引擎 |
| **列结构变更** | 需要禁用/重新启用 CDC | 自动跟踪，无需禁用 |
| **数据丢失风险** | 禁用期间丢失 DML 变更 | 无禁用操作，无数据丢失 |
| **性能影响** | 中等（需要额外存储） | 低（轻量级跟踪） |
| **DDL 支持** | 通过 `cdc.ddl_history` | 程序端字段比对检测 |
| **版本管理** | 基于 LSN (Log Sequence Number) | 基于版本号 (Version Number) |
| **数据格式** | 完整的变更数据 | 仅变更标识和变更类型 |
| **SQL Server 版本** | 2008+ (需要企业版或开发版) | 2008+ (所有版本) |

### 1.2 Change Tracking 的优势

1. **无需禁用操作**：列结构变更后自动跟踪，无需禁用/重新启用
2. **无数据丢失**：不存在禁用期间的数据丢失问题
3. **轻量级**：性能开销小于 CDC
4. **版本兼容**：支持所有 SQL Server 版本（包括标准版）
5. **无需数据库权限**：DDL 检测通过程序端实现，不需要创建触发器

### 1.3 Change Tracking 的局限性

1. **不包含变更数据**：只返回变更标识和变更类型，需要 JOIN 原表获取数据
2. **DDL 检测有延迟**：通过程序端轮询检测，存在 3-5 秒延迟
3. **版本号单调递增**：无法像 LSN 那样精确反映事务时间点

## 二、核心实现架构

### 2.1 数据流程图

```
┌─────────────────────────────────────────────────────────────┐
│                    SQL Server Database                      │
│              (Change Tracking Enabled Tables)              │
│                                                             │
│  DML变更 → Change Tracking 捕获 → 版本号递增                │
│  DDL变更 → 程序端轮询表结构 → 比对差异 → 生成 DDL           │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            │ VersionPuller 轮询
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    VersionPuller (单例)                    │
│                    Worker Thread                            │
│                                                             │
│  每 100ms 轮询:                                             │
│  1. 获取 maxVersion = CHANGE_TRACKING_CURRENT_VERSION()    │
│  2. 比较 maxVersion > listener.getLastVersion()             │
│  3. listener.pushStopVersion(maxVersion)                   │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            │ pushStopVersion()
                            ▼
┌─────────────────────────────────────────────────────────────┐
│         SqlServerCTListener.Buffer Queue                   │
│         BlockingQueue<Long> (容量: 256)                    │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            │ buffer.take() (阻塞)
                            ▼
┌─────────────────────────────────────────────────────────────┐
│         SqlServerCTListener.Worker Thread                   │
│                                                             │
│  1. 从队列取出版本号 (合并多个)                              │
│  2. 验证版本号有效性                                         │
│  3. pull(stopVersion)                                       │
│     - 查询 DML 变更（CHANGETABLE）                           │
│     - JOIN 原表获取完整数据                                  │
│     - 从 DDL 队列获取 DDL 变更                               │
│     - 合并并按版本号排序                                      │
│     - 按顺序解析和发送                                        │
│  4. 统一更新 lastVersion 并保存到 snapshot                   │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            │ sendChangedEvent()
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    AbstractListener                         │
│                                                             │
│  changeEvent() → watcher.changeEvent()                      │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│         DDLDetector (独立线程)                              │
│                                                             │
│  每 3-5 秒轮询:                                             │
│  1. 查询所有表的当前结构（INFORMATION_SCHEMA）              │
│  2. 与上次快照比对差异                                       │
│  3. 生成 DDL 语句                                           │
│  4. 获取当前版本号并创建 DDL 事件                            │
│  5. 加入 DDL 事件队列                                        │
│  6. 更新表结构快照                                           │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 关键组件

| 组件                  | 职责                | 位置                                                       |
| ------------------- | ----------------- | -------------------------------------------------------- |
| `SqlServerCTListener` | 监听器主类，管理 CT 生命周期 | `org.dbsyncer.connector.sqlserver.ct.SqlServerCTListener` |
| `VersionPuller`     | 全局版本号轮询器，单例模式   | `org.dbsyncer.connector.sqlserver.ct.VersionPuller`       |
| `Worker`            | 工作线程，处理变更数据       | `SqlServerCTListener.Worker`                             |
| `CTEvent`           | CT 事件封装          | `org.dbsyncer.connector.sqlserver.model.CTEvent`          |
| `UnifiedChangeEvent`| 统一事件模型，合并 DDL 和 DML | `org.dbsyncer.connector.sqlserver.model.UnifiedChangeEvent` |
| `DDLDetector`       | DDL 检测器，定期轮询表结构     | `SqlServerCTListener.DDLDetector`                           |

## 三、Change Tracking 启用和配置

### 3.1 数据库级别启用

```sql
-- 启用数据库级别的 Change Tracking
ALTER DATABASE [database_name]
SET CHANGE_TRACKING = ON
(
    CHANGE_RETENTION = 2 DAYS,  -- 变更保留时间，建议 2-7 天
    AUTO_CLEANUP = ON           -- 自动清理过期的变更记录
);
```

**参数说明**：
- `CHANGE_RETENTION`：变更信息的保留时间，超过此时间的变更会被自动清理
- `AUTO_CLEANUP`：是否自动清理过期的变更记录

### 3.2 表级别启用

```sql
-- 启用表的 Change Tracking
ALTER TABLE [schema_name].[table_name]
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = ON);
```

**参数说明**：
- `TRACK_COLUMNS_UPDATED`：是否跟踪哪些列被更新（用于优化查询性能）

### 3.3 检查 Change Tracking 状态

```sql
-- 检查数据库是否启用 Change Tracking
SELECT is_change_tracking_on 
FROM sys.databases 
WHERE name = 'database_name';

-- 检查表是否启用 Change Tracking
SELECT 
    t.name AS table_name,
    t.is_tracked_by_cdc AS is_change_tracking_enabled
FROM sys.tables t
WHERE t.name = 'table_name';
```

**注意**：`sys.tables.is_tracked_by_cdc` 字段名称有误导性，实际上也用于 Change Tracking。

## 四、DML 变更捕获

### 4.1 Change Tracking 版本号

Change Tracking 使用版本号（BIGINT）来标识变更，版本号单调递增。

```sql
-- 获取当前数据库的 Change Tracking 版本号
SELECT CHANGE_TRACKING_CURRENT_VERSION();
```

### 4.2 查询 DML 变更

使用 `CHANGETABLE` 函数查询变更：

```sql
-- 查询自指定版本号以来的所有变更
SELECT 
    CT.SYS_CHANGE_VERSION,      -- 变更版本号
    CT.SYS_CHANGE_CREATION_VERSION,  -- 创建版本号（INSERT 操作）
    CT.SYS_CHANGE_OPERATION,    -- 变更类型：'I'=INSERT, 'U'=UPDATE, 'D'=DELETE
    CT.SYS_CHANGE_COLUMNS,      -- 被更新的列（如果 TRACK_COLUMNS_UPDATED = ON）
    CT.SYS_CHANGE_CONTEXT,      -- 变更上下文（可选）
    T.*                          -- 原表的完整数据（需要 JOIN）
FROM CHANGETABLE(CHANGES [schema_name].[table_name], @lastVersion) AS CT
LEFT JOIN [schema_name].[table_name] AS T 
    ON CT.[primary_key_column] = T.[primary_key_column]
WHERE CT.SYS_CHANGE_VERSION > @lastVersion 
    AND CT.SYS_CHANGE_VERSION <= @stopVersion
ORDER BY CT.SYS_CHANGE_VERSION ASC;
```

**关键点**：
- `CHANGETABLE(CHANGES table_name, @lastVersion)` 返回自 `@lastVersion` 以来的所有变更
- `SYS_CHANGE_OPERATION`：'I'=INSERT, 'U'=UPDATE, 'D'=DELETE
- 需要 JOIN 原表获取完整数据（DELETE 操作无法 JOIN，需要通过快照表获取）
- 变更按版本号升序排序

### 4.3 处理 DELETE 操作

DELETE 操作无法通过 JOIN 原表获取数据，需要：

1. **方案 A：使用快照表**：在删除前将数据保存到快照表
2. **方案 B：使用主键查询历史**：通过主键查询变更前的数据（需要额外的历史表）
3. **方案 C：只发送主键**：DELETE 事件只包含主键信息，由上层处理

**推荐方案 C**：DELETE 事件只包含主键，由上层应用根据业务需求处理。

### 4.4 主键处理

Change Tracking 需要表有主键，查询变更时需要指定主键列：

```sql
-- 获取表的主键列
SELECT 
    COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = 'schema_name'
    AND TABLE_NAME = 'table_name'
    AND CONSTRAINT_NAME LIKE 'PK%'
ORDER BY ORDINAL_POSITION;
```

**复合主键处理**：
- 如果表有复合主键，需要构建多个 JOIN 条件
- 例如：`CT.[col1] = T.[col1] AND CT.[col2] = T.[col2]`

## 五、DDL 变更捕获

### 5.1 程序端字段比对方案

Change Tracking 不直接支持 DDL 变更跟踪，通过程序端定期轮询表结构并比对差异来检测 DDL 变更：

#### 5.1.1 表结构查询

使用 `INFORMATION_SCHEMA.COLUMNS` 查询表结构：

```sql
-- 查询表的完整列信息
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @schemaName
    AND TABLE_NAME = @tableName
ORDER BY ORDINAL_POSITION;

-- 查询主键信息
SELECT 
    COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = @schemaName
    AND TABLE_NAME = @tableName
    AND CONSTRAINT_NAME LIKE 'PK%'
ORDER BY ORDINAL_POSITION;
```

#### 5.1.2 表结构快照存储

将表结构序列化为 JSON 并保存到 `snapshot`：

```java
// 表结构快照的键名格式
private static final String SCHEMA_SNAPSHOT_PREFIX = "schema_snapshot_";

// 表结构模型
public class TableSchema {
    private String tableName;
    private List<ColumnInfo> columns;
    private List<String> primaryKeys;
    private Long snapshotVersion;  // 快照时的 Change Tracking 版本号
    private Date snapshotTime;     // 快照时间
}

public class ColumnInfo {
    private String columnName;
    private String dataType;
    private Integer maxLength;
    private Integer precision;
    private Integer scale;
    private Boolean nullable;
    private String defaultValue;
    private Integer ordinalPosition;
}
```

#### 5.1.3 表结构比对逻辑

```java
/**
 * 比对两个表结构的差异
 */
public List<DDLChange> compareTableSchema(TableSchema oldSchema, TableSchema newSchema) {
    List<DDLChange> changes = new ArrayList<>();
    
    // 1. 检测新增列
    Map<String, ColumnInfo> oldColumns = oldSchema.getColumns().stream()
        .collect(Collectors.toMap(ColumnInfo::getColumnName, c -> c));
    Map<String, ColumnInfo> newColumns = newSchema.getColumns().stream()
        .collect(Collectors.toMap(ColumnInfo::getColumnName, c -> c));
    
    for (ColumnInfo newCol : newSchema.getColumns()) {
        if (!oldColumns.containsKey(newCol.getColumnName())) {
            // 新增列
            String ddl = generateAddColumnDDL(newSchema.getTableName(), newCol);
            changes.add(new DDLChange("ADD_COLUMN", ddl, newCol.getColumnName()));
        }
    }
    
    // 2. 检测删除列
    for (ColumnInfo oldCol : oldSchema.getColumns()) {
        if (!newColumns.containsKey(oldCol.getColumnName())) {
            // 删除列
            String ddl = generateDropColumnDDL(newSchema.getTableName(), oldCol);
            changes.add(new DDLChange("DROP_COLUMN", ddl, oldCol.getColumnName()));
        }
    }
    
    // 3. 检测修改列（类型、长度、精度、可空性、默认值）
    for (ColumnInfo newCol : newSchema.getColumns()) {
        ColumnInfo oldCol = oldColumns.get(newCol.getColumnName());
        if (oldCol != null && !isColumnEqual(oldCol, newCol)) {
            // 列属性变更
            String ddl = generateAlterColumnDDL(newSchema.getTableName(), oldCol, newCol);
            changes.add(new DDLChange("ALTER_COLUMN", ddl, newCol.getColumnName()));
        }
    }
    
    // 4. 检测主键变更（可选，通常不常见）
    if (!oldSchema.getPrimaryKeys().equals(newSchema.getPrimaryKeys())) {
        String ddl = generateAlterPrimaryKeyDDL(newSchema.getTableName(), 
            oldSchema.getPrimaryKeys(), newSchema.getPrimaryKeys());
        changes.add(new DDLChange("ALTER_PRIMARY_KEY", ddl, null));
    }
    
    return changes;
}

/**
 * 判断两个列是否相等
 */
private boolean isColumnEqual(ColumnInfo oldCol, ColumnInfo newCol) {
    return Objects.equals(oldCol.getDataType(), newCol.getDataType())
        && Objects.equals(oldCol.getMaxLength(), newCol.getMaxLength())
        && Objects.equals(oldCol.getPrecision(), newCol.getPrecision())
        && Objects.equals(oldCol.getScale(), newCol.getScale())
        && Objects.equals(oldCol.getNullable(), newCol.getNullable())
        && Objects.equals(normalizeDefaultValue(oldCol.getDefaultValue()), 
                         normalizeDefaultValue(newCol.getDefaultValue()));
}
```

#### 5.1.4 DDL 语句生成

```java
/**
 * 生成 ADD COLUMN 的 DDL
 */
private String generateAddColumnDDL(String tableName, ColumnInfo column) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("ALTER TABLE [").append(schema).append("].[").append(tableName).append("] ");
    ddl.append("ADD [").append(column.getColumnName()).append("] ");
    ddl.append(column.getDataType());
    
    // 处理长度/精度
    if (column.getMaxLength() != null && column.getMaxLength() > 0) {
        if ("nvarchar".equalsIgnoreCase(column.getDataType()) 
            || "varchar".equalsIgnoreCase(column.getDataType())
            || "nchar".equalsIgnoreCase(column.getDataType())
            || "char".equalsIgnoreCase(column.getDataType())) {
            ddl.append("(").append(column.getMaxLength()).append(")");
        }
    } else if (column.getPrecision() != null && column.getScale() != null) {
        ddl.append("(").append(column.getPrecision()).append(",")
           .append(column.getScale()).append(")");
    }
    
    // 处理可空性
    if (Boolean.FALSE.equals(column.getNullable())) {
        ddl.append(" NOT NULL");
    }
    
    // 处理默认值
    if (column.getDefaultValue() != null && !column.getDefaultValue().isEmpty()) {
        ddl.append(" DEFAULT ").append(column.getDefaultValue());
    }
    
    return ddl.toString();
}

/**
 * 生成 DROP COLUMN 的 DDL
 */
private String generateDropColumnDDL(String tableName, ColumnInfo column) {
    return String.format("ALTER TABLE [%s].[%s] DROP COLUMN [%s]", 
        schema, tableName, column.getColumnName());
}

/**
 * 生成 ALTER COLUMN 的 DDL
 */
private String generateAlterColumnDDL(String tableName, ColumnInfo oldCol, ColumnInfo newCol) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("ALTER TABLE [").append(schema).append("].[").append(tableName).append("] ");
    ddl.append("ALTER COLUMN [").append(newCol.getColumnName()).append("] ");
    ddl.append(newCol.getDataType());
    
    // 处理长度/精度
    if (newCol.getMaxLength() != null && newCol.getMaxLength() > 0) {
        if ("nvarchar".equalsIgnoreCase(newCol.getDataType()) 
            || "varchar".equalsIgnoreCase(newCol.getDataType())
            || "nchar".equalsIgnoreCase(newCol.getDataType())
            || "char".equalsIgnoreCase(newCol.getDataType())) {
            ddl.append("(").append(newCol.getMaxLength()).append(")");
        }
    } else if (newCol.getPrecision() != null && newCol.getScale() != null) {
        ddl.append("(").append(newCol.getPrecision()).append(",")
           .append(newCol.getScale()).append(")");
    }
    
    // 处理可空性
    if (Boolean.FALSE.equals(newCol.getNullable())) {
        ddl.append(" NOT NULL");
    } else {
        ddl.append(" NULL");
    }
    
    // 注意：ALTER COLUMN 不能直接修改默认值，需要先删除再添加
    // 这里只生成列类型和可空性的变更
    
    return ddl.toString();
}
```

#### 5.1.5 DDL 检测轮询机制

```java
/**
 * DDL 检测器（定期轮询表结构）
 */
private class DDLDetector {
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final long pollInterval = 5000;  // 轮询间隔（默认 5 秒）
    private final BlockingQueue<DDLEvent> ddlEventQueue = new LinkedBlockingQueue<>();
    
    public void start() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                detectDDLChanges();
            } catch (Exception e) {
                logger.error("DDL 检测失败", e);
            }
        }, pollInterval, pollInterval, TimeUnit.MILLISECONDS);
    }
    
    private void detectDDLChanges() throws Exception {
        Long currentVersion = getCurrentVersion();  // 获取当前 Change Tracking 版本号
        
        for (String tableName : tables) {
            // 1. 查询当前表结构
            TableSchema currentSchema = queryTableSchema(tableName);
            currentSchema.setSnapshotVersion(currentVersion);
            currentSchema.setSnapshotTime(new Date());
            
            // 2. 读取上次保存的表结构快照
            TableSchema lastSchema = loadTableSchemaSnapshot(tableName);
            
            // 3. 如果是首次检测，保存快照并跳过
            if (lastSchema == null) {
                saveTableSchemaSnapshot(tableName, currentSchema);
                continue;
            }
            
            // 4. 比对差异
            List<DDLChange> changes = compareTableSchema(lastSchema, currentSchema);
            
            // 5. 如果有变更，生成 DDL 事件并发送
            if (!changes.isEmpty()) {
                for (DDLChange change : changes) {
                    // 使用当前版本号作为 DDL 事件的版本号
                    DDLEvent ddlEvent = new DDLEvent(
                        tableName, 
                        change.getDdlCommand(), 
                        currentVersion,  // 使用当前 Change Tracking 版本号
                        new Date()
                    );
                    
                    // 将 DDL 事件加入待处理队列（与 DML 事件合并处理）
                    ddlEventQueue.offer(ddlEvent);
                }
                
                // 6. 更新表结构快照
                saveTableSchemaSnapshot(tableName, currentSchema);
                
                logger.info("检测到表 {} 的 DDL 变更，共 {} 个变更", tableName, changes.size());
            }
        }
    }
    
    public BlockingQueue<DDLEvent> getDdlEventQueue() {
        return ddlEventQueue;
    }
}
```

#### 5.1.6 表结构快照持久化

```java
/**
 * 保存表结构快照到 snapshot
 */
private void saveTableSchemaSnapshot(String tableName, TableSchema schema) throws Exception {
    String key = SCHEMA_SNAPSHOT_PREFIX + tableName;
    String json = JsonUtil.objToJson(schema);
    snapshot.put(key, json);
    super.forceFlushEvent();
}

/**
 * 从 snapshot 加载表结构快照
 */
private TableSchema loadTableSchemaSnapshot(String tableName) throws Exception {
    String key = SCHEMA_SNAPSHOT_PREFIX + tableName;
    String json = snapshot.get(key);
    if (json == null || json.isEmpty()) {
        return null;
    }
    return JsonUtil.jsonToObj(json, TableSchema.class);
}

/**
 * 查询表的当前结构
 */
private TableSchema queryTableSchema(String tableName) throws Exception {
    // 查询列信息
    String sql = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, " +
                 "NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION " +
                 "FROM INFORMATION_SCHEMA.COLUMNS " +
                 "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
                 "ORDER BY ORDINAL_POSITION";
    
    List<ColumnInfo> columns = queryAndMapList(sql, statement -> {
        statement.setString(1, schema);
        statement.setString(2, tableName);
    }, rs -> {
        List<ColumnInfo> cols = new ArrayList<>();
        while (rs.next()) {
            ColumnInfo col = new ColumnInfo();
            col.setColumnName(rs.getString("COLUMN_NAME"));
            col.setDataType(rs.getString("DATA_TYPE"));
            col.setMaxLength(rs.getObject("CHARACTER_MAXIMUM_LENGTH") != null 
                ? rs.getInt("CHARACTER_MAXIMUM_LENGTH") : null);
            col.setPrecision(rs.getObject("NUMERIC_PRECISION") != null 
                ? rs.getInt("NUMERIC_PRECISION") : null);
            col.setScale(rs.getObject("NUMERIC_SCALE") != null 
                ? rs.getInt("NUMERIC_SCALE") : null);
            col.setNullable("YES".equalsIgnoreCase(rs.getString("IS_NULLABLE")));
            col.setDefaultValue(rs.getString("COLUMN_DEFAULT"));
            col.setOrdinalPosition(rs.getInt("ORDINAL_POSITION"));
            cols.add(col);
        }
        return cols;
    });
    
    // 查询主键
    String pkSql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                   "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME LIKE 'PK%' " +
                   "ORDER BY ORDINAL_POSITION";
    
    List<String> primaryKeys = queryAndMapList(pkSql, statement -> {
        statement.setString(1, schema);
        statement.setString(2, tableName);
    }, rs -> {
        List<String> pks = new ArrayList<>();
        while (rs.next()) {
            pks.add(rs.getString("COLUMN_NAME"));
        }
        return pks;
    });
    
    TableSchema schema = new TableSchema();
    schema.setTableName(tableName);
    schema.setColumns(columns);
    schema.setPrimaryKeys(primaryKeys);
    
    return schema;
}
```

### 5.2 DDL 事件与 DML 事件合并

```java
/**
 * 合并 DDL 和 DML 事件（按版本号排序）
 */
private void mergeAndProcessEvents(Long stopVersion) throws Exception {
    // 1. 查询 DML 变更
    List<CTEvent> dmlEvents = pullDMLChanges(stopVersion);
    
    // 2. 从 DDL 队列中取出所有待处理的 DDL 事件
    List<DDLEvent> ddlEvents = new ArrayList<>();
    BlockingQueue<DDLEvent> ddlQueue = ddlDetector.getDdlEventQueue();
    DDLEvent ddlEvent;
    while ((ddlEvent = ddlQueue.poll()) != null) {
        if (ddlEvent.getVersion() <= stopVersion) {
            ddlEvents.add(ddlEvent);
        } else {
            // 版本号超过 stopVersion，放回队列
            ddlQueue.offer(ddlEvent);
            break;
        }
    }
    
    // 3. 合并并按版本号排序
    List<UnifiedChangeEvent> unifiedEvents = new ArrayList<>();
    
    // 添加 DDL 事件
    for (DDLEvent ddlEvent : ddlEvents) {
        unifiedEvents.add(new UnifiedChangeEvent(
            "DDL", 
            ddlEvent.getTableName(), 
            ddlEvent.getDdlCommand(), 
            null, 
            ddlEvent.getVersion()
        ));
    }
    
    // 添加 DML 事件
    for (CTEvent dmlEvent : dmlEvents) {
        unifiedEvents.add(new UnifiedChangeEvent(
            "DML", 
            dmlEvent.getTableName(), 
            null, 
            dmlEvent, 
            dmlEvent.getVersion()
        ));
    }
    
    // 按版本号排序
    unifiedEvents.sort(Comparator.comparing(UnifiedChangeEvent::getVersion));
    
    // 4. 按顺序处理事件
    for (UnifiedChangeEvent event : unifiedEvents) {
        if ("DDL".equals(event.getType())) {
            parseDDLEvent(event);
        } else {
            parseDMLEvent(event);
        }
    }
}
```

**关键点**：
- DDL 变更通过程序端轮询检测，使用 Change Tracking 版本号进行排序
- 确保 DDL 和 DML 变更可以按版本号合并排序
- 表结构快照保存到 `snapshot`，支持断点续传
- 首次检测时只保存快照，不生成 DDL 事件

## 六、版本号管理

### 6.1 版本号获取机制

```java
/**
 * 获取最大版本号（仅考虑 DML，DDL 事件使用检测时的版本号）
 */
public Long getMaxVersion() {
    // 获取 Change Tracking 当前版本号（DML）
    Long dmlMaxVersion = queryAndMap(
        "SELECT CHANGE_TRACKING_CURRENT_VERSION()", 
        rs -> rs.getLong(1)
    );
    
    // DDL 事件通过程序端检测，使用检测时的版本号
    // 不需要单独查询 DDL 版本号，因为 DDL 事件已经包含版本号信息
    
    return dmlMaxVersion;
}
```

### 6.2 版本号持久化

使用 `snapshot` 持久化最后处理的版本号：

```java
private static final String VERSION_POSITION = "version";

private void readLastVersion() throws Exception {
    if (!snapshot.containsKey(VERSION_POSITION)) {
        lastVersion = getMaxVersion();
        if (lastVersion != null) {
            snapshot.put(VERSION_POSITION, String.valueOf(lastVersion));
            super.forceFlushEvent();
            return;
        }
        throw new SqlServerException("No Change Tracking version available");
    }
    lastVersion = Long.valueOf(snapshot.get(VERSION_POSITION));
}
```

### 6.3 版本号更新

```java
// 统一更新版本号（DDL 和 DML 共享同一个版本号）
lastVersion = stopVersion;
snapshot.put(VERSION_POSITION, String.valueOf(lastVersion));
```

## 七、核心实现细节

### 7.1 SQL 常量定义

```java
// Change Tracking 相关 SQL
private static final String GET_CURRENT_VERSION = 
    "SELECT CHANGE_TRACKING_CURRENT_VERSION()";

private static final String GET_DML_CHANGES = 
    "SELECT " +
    "    CT.SYS_CHANGE_VERSION, " +
    "    CT.SYS_CHANGE_OPERATION, " +
    "    CT.SYS_CHANGE_COLUMNS, " +
    "    T.* " +
    "FROM CHANGETABLE(CHANGES [%s].[%s], ?) AS CT " +
    "LEFT JOIN [%s].[%s] AS T ON CT.[%s] = T.[%s] " +
    "WHERE CT.SYS_CHANGE_VERSION > ? AND CT.SYS_CHANGE_VERSION <= ? " +
    "ORDER BY CT.SYS_CHANGE_VERSION ASC";

// 表结构查询 SQL
private static final String GET_TABLE_COLUMNS = 
    "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, " +
    "NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION " +
    "FROM INFORMATION_SCHEMA.COLUMNS " +
    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
    "ORDER BY ORDINAL_POSITION";

private static final String GET_TABLE_PRIMARY_KEYS = 
    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME LIKE 'PK%' " +
    "ORDER BY ORDINAL_POSITION";

// 启用 Change Tracking
private static final String ENABLE_DB_CHANGE_TRACKING = 
    "ALTER DATABASE [%s] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)";

private static final String ENABLE_TABLE_CHANGE_TRACKING = 
    "ALTER TABLE [%s].[%s] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)";

private static final String IS_DB_CHANGE_TRACKING_ENABLED = 
    "SELECT is_change_tracking_on FROM sys.databases WHERE name = '%s'";

private static final String IS_TABLE_CHANGE_TRACKING_ENABLED = 
    "SELECT is_tracked_by_cdc FROM sys.tables WHERE name = '%s'";
```

### 7.2 DML 变更查询（pullDMLChanges）

```java
private List<CTEvent> pullDMLChanges(String tableName, Long startVersion, Long stopVersion) throws Exception {
    // 1. 获取表的主键列
    List<String> primaryKeys = getPrimaryKeys(tableName);
    if (primaryKeys.isEmpty()) {
        throw new SqlServerException("表 " + tableName + " 没有主键，无法使用 Change Tracking");
    }
    
    // 2. 构建 JOIN 条件
    String joinCondition = primaryKeys.stream()
        .map(pk -> "CT.[" + pk + "] = T.[" + pk + "]")
        .collect(Collectors.joining(" AND "));
    
    // 3. 构建查询 SQL
    String sql = String.format(GET_DML_CHANGES, 
        schema, tableName,  // CHANGETABLE
        schema, tableName,  // JOIN table
        primaryKeys.get(0), primaryKeys.get(0)  // JOIN condition
    );
    
    // 4. 查询变更
    return queryAndMapList(sql, statement -> {
        statement.setLong(1, startVersion);   // CHANGETABLE 起始版本
        statement.setLong(2, startVersion);   // WHERE 起始版本
        statement.setLong(3, stopVersion);    // WHERE 结束版本
    }, rs -> {
        List<CTEvent> events = new ArrayList<>();
        while (rs.next()) {
            Long version = rs.getLong(1);
            String operation = rs.getString(2);  // 'I', 'U', 'D'
            byte[] columnsUpdated = rs.getBytes(3);  // 更新的列（二进制）
            
            // 构建行数据
            List<Object> row = new ArrayList<>();
            for (int i = 4; i <= rs.getMetaData().getColumnCount(); i++) {
                row.add(rs.getObject(i));
            }
            
            // 转换操作类型
            int operationCode = convertOperation(operation);
            
            events.add(new CTEvent(tableName, operationCode, row, version, columnsUpdated));
        }
        return events;
    });
}
```

### 7.3 DDL 变更检测（pullDDLChanges）

```java
/**
 * DDL 变更通过 DDLDetector 异步检测，这里从队列中获取
 */
private List<DDLEvent> pullDDLChanges(Long startVersion, Long stopVersion) throws Exception {
    List<DDLEvent> events = new ArrayList<>();
    BlockingQueue<DDLEvent> ddlQueue = ddlDetector.getDdlEventQueue();
    
    // 从队列中取出所有在版本范围内的 DDL 事件
    DDLEvent ddlEvent;
    while ((ddlEvent = ddlQueue.poll()) != null) {
        if (ddlEvent.getVersion() > startVersion && ddlEvent.getVersion() <= stopVersion) {
            // 检查表名是否匹配
            if (filterTable.contains(ddlEvent.getTableName())) {
                events.add(ddlEvent);
            }
        } else if (ddlEvent.getVersion() > stopVersion) {
            // 版本号超过范围，放回队列
            ddlQueue.offer(ddlEvent);
            break;
        }
        // 版本号小于等于 startVersion 的事件直接丢弃（已处理过）
    }
    
    // 按版本号排序
    events.sort(Comparator.comparing(DDLEvent::getVersion));
    
    return events;
}
```

### 7.4 变更合并和排序

```java
private void pull(Long stopVersion) throws Exception {
    // 1. 查询 DML 变更
    List<CTEvent> dmlEvents = new ArrayList<>();
    for (String table : tables) {
        List<CTEvent> tableEvents = pullDMLChanges(table, lastVersion, stopVersion);
        dmlEvents.addAll(tableEvents);
    }
    
    // 2. 查询 DDL 变更
    List<DDLEvent> ddlEvents = pullDDLChanges(lastVersion, stopVersion);
    
    // 3. 合并并按版本号排序
    List<UnifiedChangeEvent> unifiedEvents = mergeAndSortEvents(ddlEvents, dmlEvents);
    
    // 4. 按顺序解析和发送
    parseUnifiedEvents(unifiedEvents, stopVersion);
}

private List<UnifiedChangeEvent> mergeAndSortEvents(
        List<DDLEvent> ddlEvents, 
        List<CTEvent> dmlEvents) {
    List<UnifiedChangeEvent> unifiedEvents = new ArrayList<>();
    
    // 添加 DDL 事件
    for (DDLEvent ddlEvent : ddlEvents) {
        unifiedEvents.add(new UnifiedChangeEvent(
            "DDL", 
            ddlEvent.getTableName(), 
            ddlEvent.getDdlCommand(), 
            null, 
            ddlEvent.getVersion()
        ));
    }
    
    // 添加 DML 事件
    for (CTEvent dmlEvent : dmlEvents) {
        unifiedEvents.add(new UnifiedChangeEvent(
            "DML", 
            dmlEvent.getTableName(), 
            null, 
            dmlEvent, 
            dmlEvent.getVersion()
        ));
    }
    
    // 按版本号排序
    unifiedEvents.sort(Comparator.comparing(UnifiedChangeEvent::getVersion));
    
    return unifiedEvents;
}
```

## 八、与现有架构的集成

### 8.1 版本号轮询器（VersionPuller）

类似于 `LsnPuller`，创建 `VersionPuller` 负责轮询版本号：

```java
public class VersionPuller {
    private static final VersionPuller INSTANCE = new VersionPuller();
    private final Map<String, SqlServerCTListener> extractors = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    public static VersionPuller getInstance() {
        return INSTANCE;
    }
    
    public void addExtractor(String metaId, SqlServerCTListener listener) {
        extractors.put(metaId, listener);
    }
    
    public void removeExtractor(String metaId) {
        extractors.remove(metaId);
    }
    
    public void start() {
        scheduler.scheduleAtFixedRate(() -> {
            extractors.values().forEach(listener -> {
                try {
                    Long maxVersion = listener.getMaxVersion();
                    if (maxVersion != null && maxVersion > listener.getLastVersion()) {
                        listener.pushStopVersion(maxVersion);
                    }
                } catch (Exception e) {
                    logger.error("VersionPuller 轮询失败", e);
                }
            });
        }, 0, 100, TimeUnit.MILLISECONDS);
    }
}
```

### 8.2 监听器实现（SqlServerCTListener）

```java
public class SqlServerCTListener extends AbstractDatabaseListener {
    private Long lastVersion;
    private final BlockingQueue<Long> buffer = new LinkedBlockingQueue<>(256);
    private Worker worker;
    
    @Override
    public void start() throws Exception {
        connectLock.lock();
        try {
            if (connected) {
                return;
            }
            connected = true;
            connect();
            readTables();
            
            enableDBChangeTracking();
            enableTableChangeTracking();
            readLastVersion();
            
            worker = new Worker();
            worker.start();
            VersionPuller.getInstance().addExtractor(metaId, this);
        } finally {
            connectLock.unlock();
        }
    }
    
    public Long getMaxVersion() {
        // 实现版本号获取逻辑
    }
    
    public Long getLastVersion() {
        return lastVersion;
    }
    
    public void pushStopVersion(Long version) {
        if (!buffer.contains(version)) {
            buffer.offer(version);
        }
    }
    
    private void pull(Long stopVersion) {
        // 实现变更查询逻辑
    }
}
```

## 九、迁移方案

### 9.1 从 CDC 迁移到 Change Tracking

1. **创建新的监听器类**：`SqlServerCTListener`（与 `SqlServerListener` 并行）
2. **配置选择**：通过配置选择使用 CDC 或 CT
3. **数据迁移**：将 CDC 的 `lastLsn` 转换为 CT 的版本号（需要映射表）
4. **逐步切换**：支持同时运行，逐步迁移

### 9.2 兼容性处理

- 保持相同的接口：`AbstractDatabaseListener`
- 统一事件模型：`UnifiedChangeEvent`
- 版本号映射：提供 LSN 到版本号的映射工具（可选）

## 十、关键注意事项

### 10.1 主键要求

- Change Tracking 要求表必须有主键
- 复合主键需要特殊处理 JOIN 条件

### 10.2 DELETE 操作处理

- DELETE 操作无法 JOIN 原表获取数据
- 方案：只发送主键信息，由上层应用处理

### 10.3 版本号精度

- Change Tracking 版本号是单调递增的整数
- 无法像 LSN 那样精确反映事务时间点
- 对于同一事务内的多个变更，版本号可能相同

### 10.4 DDL 检测轮询

- 轮询间隔建议 3-5 秒，过短会增加数据库负载，过长会延迟 DDL 检测
- 表结构快照保存到 `snapshot`，占用一定存储空间
- 首次检测时只保存快照，不生成 DDL 事件
- 某些复杂 DDL（如索引变更、约束变更）可能无法检测

### 10.5 变更保留时间

- `CHANGE_RETENTION` 设置建议 2-7 天
- 如果同步延迟超过保留时间，需要全量同步

## 十一、性能优化

### 11.1 批量查询

- 批量查询多个表的变更，减少数据库连接
- 使用 `IN` 子句合并表名查询

### 11.2 索引优化

- Change Tracking 内部已优化，无需额外索引
- 表结构查询使用 `INFORMATION_SCHEMA`，性能良好

### 11.3 查询优化

- 使用 `TRACK_COLUMNS_UPDATED = ON` 可以优化 UPDATE 查询
- 对于 DELETE 操作，避免 JOIN 原表
- DDL 检测只查询配置的表，避免全库扫描

## 十二、错误处理

### 12.1 版本号丢失

- 如果 `lastVersion` 丢失，从当前版本号开始（可能导致数据重复）
- 建议：记录版本号到快照，支持手动恢复

### 12.2 DDL 检测失败

- DDL 检测失败不影响 DML 同步，但会导致 DDL 变更丢失
- 建议：监控检测器状态，记录失败日志，支持手动触发检测

### 12.4 表结构快照损坏

- 如果表结构快照损坏或丢失，首次检测时会重新生成
- 建议：定期备份 `snapshot`，支持手动恢复

### 12.3 变更保留时间过期

- 如果同步延迟超过 `CHANGE_RETENTION`，变更会被清理
- 建议：触发全量同步，或增加保留时间

## 十三、测试建议

### 13.1 功能测试

- DML 变更捕获（INSERT/UPDATE/DELETE）
- DDL 变更捕获（ALTER TABLE）
- 版本号排序和合并
- 断点续传

### 13.2 性能测试

- 高并发场景下的变更捕获
- 大量表的变更查询性能
- DDL 检测轮询性能影响

### 13.3 异常测试

- 版本号丢失恢复
- DDL 检测失败处理
- 变更保留时间过期处理
- 表结构快照损坏恢复

## 十四、CDC vs CT 选择建议

### 14.1 选择 CDC 的场景

- 需要完整的变更数据（包括 DELETE 的完整数据）
- 需要精确的事务时间点（LSN）
- 已有 CDC 基础设施
- 企业版或开发版 SQL Server

### 14.2 选择 Change Tracking 的场景

- 需要避免禁用操作导致的数据丢失
- 列结构频繁变更
- 标准版 SQL Server
- 轻量级变更跟踪需求
- 可以接受 DELETE 操作只包含主键

### 14.3 混合方案

- 可以同时支持 CDC 和 CT，通过配置选择
- 不同表可以使用不同的跟踪机制
- 提供统一的接口和事件模型

