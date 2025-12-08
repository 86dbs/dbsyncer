# SQL Server Change Tracking (CT) 同步方案

## 一、概述

基于 SQL Server Change Tracking (CT) 实现 DML 和 DDL 同步，**完全替代原有的 CDC 方案**。

**重要说明**：
- 本方案完全替代 SQL Server CDC，不再使用 CDC 相关代码
- 原有的 `SqlServerListener`（CDC 实现）将被 `SqlServerCTListener`（CT 实现）替代
- 如需使用新方案，按新方式重新同步即可，无需迁移

**核心特性**：
- 不需要 SQL Server Agent
- 列结构变更后自动跟踪，无需禁用/重新启用
- 无数据丢失风险
- 支持所有 SQL Server 版本（包括标准版）
- DDL 检测通过程序端实现，不需要创建触发器

**关键限制**：
- 表必须有主键
- 只返回变更标识和变更类型，需要 JOIN 原表获取数据
- DELETE 操作无法 JOIN 原表获取完整数据
- DDL 检测依赖 DML 查询，如果表长时间没有 DML 变更，DDL 变更会在下次 DML 查询时检测到

## 二、核心实现架构

### 2.1 数据流程图

```
┌─────────────────────────────────────────────────────────────┐
│                    SQL Server Database                      │
│              (Change Tracking Enabled Tables)              │
│                                                             │
│  DML变更 → Change Tracking 捕获 → 版本号递增                │
│  DDL变更 → DML查询时检测哈希值 → 比对差异 → 生成 DDL         │
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
│     - 检测表结构哈希值变化（ResultSetMetaData）              │
│     - 哈希值变化时查询 INFORMATION_SCHEMA 进行完整比对       │
│     - 生成 DDL 事件并与 DML 事件合并                          │
│     - 按版本号排序并发送                                      │
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

```

### 2.2 关键组件

- `SqlServerCTListener`：监听器主类，管理 CT 生命周期
- `VersionPuller`：全局版本号轮询器（单例）

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

**注意**：`sys.tables.is_tracked_by_cdc` 字段名称有误导性，实际上用于 Change Tracking（不是 CDC）。

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

DELETE 操作无法通过 JOIN 原表获取数据，DELETE 事件只包含主键信息，由上层应用处理。

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

Change Tracking 不直接支持 DDL 变更跟踪，通过在 DML 查询过程中检测表结构哈希值变化，并比对差异来检测 DDL 变更：

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

将表结构序列化为 JSON 并保存到 `snapshot` Map 中（`Map<String, String>`）：

```java
// 表结构快照的键名格式
private static final String SCHEMA_SNAPSHOT_PREFIX = "schema_snapshot_";
private static final String SCHEMA_HASH_PREFIX = "schema_hash_";           // 表结构哈希值
private static final String SCHEMA_VERSION_PREFIX = "schema_version_";      // 快照版本号
private static final String SCHEMA_TIME_PREFIX = "schema_time_";           // 快照时间
private static final String SCHEMA_ORDINAL_PREFIX = "schema_ordinal_";     // 列位置映射

// 使用现有的 MetaInfo 类存储表结构（org.dbsyncer.sdk.model.MetaInfo）
// MetaInfo 包含：
// - tableType: 表类型
// - column: List<Field> 列信息（主键信息通过 Field.isPk() 获取）
// - sql: SQL 语句（可选）
// - indexType: 索引类型（可选）

// 注意：
// 1. 表结构使用 MetaInfo 存储，序列化为 JSON 保存到 snapshot
// 2. 哈希值、版本号、时间、列位置映射分别用不同的 key 存储在 snapshot 中
// 3. 主键信息不需要单独存储，可以从 MetaInfo.getColumn() 中通过 Field.isPk() 获取
// 4. 这样设计避免了新建类，符合"如无必要勿增实体"的原则
```

#### 5.1.3 表结构比对逻辑

```java
/**
 * 从 MetaInfo 中提取主键列表
 */
private List<String> getPrimaryKeysFromMetaInfo(MetaInfo metaInfo) {
    if (metaInfo == null || metaInfo.getColumn() == null) {
        return new ArrayList<>();
    }
    return metaInfo.getColumn().stream()
        .filter(Field::isPk)
        .map(Field::getName)
        .collect(Collectors.toList());
}

/**
 * 比对两个表结构的差异
 */
public List<DDLChange> compareTableSchema(String tableName, MetaInfo oldMetaInfo, MetaInfo newMetaInfo,
                                          List<String> oldPrimaryKeys, List<String> newPrimaryKeys,
                                          Map<String, Integer> oldOrdinalPositions, Map<String, Integer> newOrdinalPositions) {
    List<DDLChange> changes = new ArrayList<>();
    
    // 1. 检测新增列
    Map<String, Field> oldColumns = oldMetaInfo.getColumn().stream()
        .collect(Collectors.toMap(Field::getName, c -> c));
    Map<String, Field> newColumns = newMetaInfo.getColumn().stream()
        .collect(Collectors.toMap(Field::getName, c -> c));
    
    for (Field newCol : newMetaInfo.getColumn()) {
        if (!oldColumns.containsKey(newCol.getName())) {
            // 新增列
            String ddl = generateAddColumnDDL(tableName, newCol);
            changes.add(new DDLChange("ADD_COLUMN", ddl, newCol.getName()));
        }
    }
    
    // 2. 检测删除列
    for (Field oldCol : oldMetaInfo.getColumn()) {
        if (!newColumns.containsKey(oldCol.getName())) {
            // 删除列
            String ddl = generateDropColumnDDL(tableName, oldCol);
            changes.add(new DDLChange("DROP_COLUMN", ddl, oldCol.getName()));
        }
    }
    
    // 3. 检测修改列（类型、长度、精度、可空性、默认值）
    for (Field newCol : newMetaInfo.getColumn()) {
        Field oldCol = oldColumns.get(newCol.getName());
        if (oldCol != null && !isColumnEqual(oldCol, newCol)) {
            // 列属性变更
            String ddl = generateAlterColumnDDL(tableName, oldCol, newCol);
            changes.add(new DDLChange("ALTER_COLUMN", ddl, newCol.getName()));
        }
    }
    
    // 4. 检测主键变更（可选，通常不常见）
    if (!oldPrimaryKeys.equals(newPrimaryKeys)) {
        String ddl = generateAlterPrimaryKeyDDL(tableName, oldPrimaryKeys, newPrimaryKeys);
        changes.add(new DDLChange("ALTER_PRIMARY_KEY", ddl, null));
    }
    
    return changes;
}

/**
 * 判断两个列是否相等
 */
private boolean isColumnEqual(Field oldCol, Field newCol) {
    return Objects.equals(oldCol.getTypeName(), newCol.getTypeName())
        && Objects.equals(oldCol.getColumnSize(), newCol.getColumnSize())
        && Objects.equals(oldCol.getRatio(), newCol.getRatio())
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
private String generateAddColumnDDL(String tableName, Field column) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("ALTER TABLE [").append(schema).append("].[").append(tableName).append("] ");
    ddl.append("ADD [").append(column.getName()).append("] ");
    ddl.append(column.getTypeName());
    
    // 处理长度/精度
    if (column.getColumnSize() > 0) {
        String typeName = column.getTypeName().toLowerCase();
        if (typeName.contains("varchar") || typeName.contains("char")) {
            ddl.append("(").append(column.getColumnSize()).append(")");
        }
    }
    
    // 处理数值类型的精度和小数位数
    if (column.getRatio() >= 0 && column.getColumnSize() > 0) {
        String typeName = column.getTypeName().toLowerCase();
        if (typeName.contains("decimal") || typeName.contains("numeric")) {
            ddl.append("(").append(column.getColumnSize()).append(",")
               .append(column.getRatio()).append(")");
        }
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
private String generateDropColumnDDL(String tableName, Field column) {
    return String.format("ALTER TABLE [%s].[%s] DROP COLUMN [%s]", 
        schema, tableName, column.getName());
}

/**
 * 生成 ALTER COLUMN 的 DDL
 */
private String generateAlterColumnDDL(String tableName, Field oldCol, Field newCol) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("ALTER TABLE [").append(schema).append("].[").append(tableName).append("] ");
    ddl.append("ALTER COLUMN [").append(newCol.getName()).append("] ");
    ddl.append(newCol.getTypeName());
    
    // 处理长度/精度
    if (newCol.getColumnSize() > 0) {
        String typeName = newCol.getTypeName().toLowerCase();
        if (typeName.contains("varchar") || typeName.contains("char")) {
            ddl.append("(").append(newCol.getColumnSize()).append(")");
        }
    }
    
    // 处理数值类型的精度和小数位数
    if (newCol.getRatio() >= 0 && newCol.getColumnSize() > 0) {
        String typeName = newCol.getTypeName().toLowerCase();
        if (typeName.contains("decimal") || typeName.contains("numeric")) {
            ddl.append("(").append(newCol.getColumnSize()).append(",")
               .append(newCol.getRatio()).append(")");
        }
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

#### 5.1.5 DDL 检测机制（在 DML 查询时触发）

```java
/**
 * 在 DML 查询过程中检测 DDL 变更
 */
private void detectDDLChangesInDMLQuery(String tableName, ResultSetMetaData metaData) throws Exception {
    // 1. 从 ResultSetMetaData 计算当前表结构哈希值
    String currentHash = calculateSchemaHashFromMetaData(metaData);
    String lastHash = getSchemaHash(tableName);
    
    if (lastHash == null) {
        // 首次检测，保存哈希值并初始化快照
        Map<String, Integer> ordinalPositions = new HashMap<>();
        MetaInfo currentMetaInfo = queryTableMetaInfo(tableName, ordinalPositions);
        Long currentVersion = getCurrentVersion();
        saveTableSchemaSnapshot(tableName, currentMetaInfo, currentVersion, currentHash, ordinalPositions);
        return;
    }
    
    if (!lastHash.equals(currentHash)) {
        // 2. 哈希值变化，触发完整比对
        Map<String, Integer> ordinalPositions = new HashMap<>();
        MetaInfo currentMetaInfo = queryTableMetaInfo(tableName, ordinalPositions);
        Long currentVersion = getCurrentVersion();
        MetaInfo lastMetaInfo = loadTableSchemaSnapshot(tableName);
        
        if (lastMetaInfo != null) {
            List<String> lastPrimaryKeys = getPrimaryKeysFromMetaInfo(lastMetaInfo);
            List<String> currentPrimaryKeys = getPrimaryKeysFromMetaInfo(currentMetaInfo);
            Map<String, Integer> lastOrdinalPositions = getColumnOrdinalPositions(tableName);
            
            // 3. 比对差异
            List<DDLChange> changes = compareTableSchema(tableName, lastMetaInfo, currentMetaInfo,
                                                         lastPrimaryKeys, currentPrimaryKeys,
                                                         lastOrdinalPositions, ordinalPositions);
            
            // 4. 如果有变更，生成 DDL 事件
            if (!changes.isEmpty()) {
                for (DDLChange change : changes) {
                    DDLEvent ddlEvent = new DDLEvent(
                        tableName, 
                        change.getDdlCommand(), 
                        currentVersion,  // 使用当前 Change Tracking 版本号
                        new Date()
                    );
                    
                    // 将 DDL 事件加入待处理队列（与 DML 事件合并处理）
                    ddlEventQueue.offer(ddlEvent);
                }
                
                // 5. 更新表结构快照
                saveTableSchemaSnapshot(tableName, currentMetaInfo, currentVersion, currentHash, ordinalPositions);
                
                logger.info("检测到表 {} 的 DDL 变更，共 {} 个变更", tableName, changes.size());
            } else {
                // 即使没有检测到变更，也更新哈希值（可能是精度/尺寸变化导致哈希变化但无法检测）
                snapshot.put(SCHEMA_HASH_PREFIX + tableName, currentHash);
            }
        }
    }
}

// DDL 事件队列（用于与 DML 事件合并）
private final BlockingQueue<DDLEvent> ddlEventQueue = new LinkedBlockingQueue<>();
```

#### 5.1.6 表结构快照持久化

```java
/**
 * 保存表结构快照到 snapshot
 */
private void saveTableSchemaSnapshot(String tableName, MetaInfo metaInfo, Long version, 
                                     String hash, Map<String, Integer> ordinalPositions) throws Exception {
    // 保存 MetaInfo（表结构）
    String schemaKey = SCHEMA_SNAPSHOT_PREFIX + tableName;
    String schemaJson = JsonUtil.objToJson(metaInfo);
    snapshot.put(schemaKey, schemaJson);
    
    // 保存哈希值
    String hashKey = SCHEMA_HASH_PREFIX + tableName;
    snapshot.put(hashKey, hash);
    
    // 保存版本号
    String versionKey = SCHEMA_VERSION_PREFIX + tableName;
    snapshot.put(versionKey, String.valueOf(version));
    
    // 保存时间戳
    String timeKey = SCHEMA_TIME_PREFIX + tableName;
    snapshot.put(timeKey, String.valueOf(System.currentTimeMillis()));
    
    // 保存列位置映射（序列化为 JSON）
    if (ordinalPositions != null && !ordinalPositions.isEmpty()) {
        String ordinalKey = SCHEMA_ORDINAL_PREFIX + tableName;
        String ordinalJson = JsonUtil.objToJson(ordinalPositions);
        snapshot.put(ordinalKey, ordinalJson);
    }
    
    super.forceFlushEvent();
}

/**
 * 从 snapshot 加载表结构快照
 */
private MetaInfo loadTableSchemaSnapshot(String tableName) throws Exception {
    String key = SCHEMA_SNAPSHOT_PREFIX + tableName;
    String json = snapshot.get(key);
    if (json == null || json.isEmpty()) {
        return null;
    }
    return JsonUtil.jsonToObj(json, MetaInfo.class);
}

/**
 * 从 snapshot 获取哈希值
 */
private String getSchemaHash(String tableName) {
    String key = SCHEMA_HASH_PREFIX + tableName;
    return snapshot.get(key);
}

/**
 * 从 snapshot 获取快照版本号
 */
private Long getSchemaSnapshotVersion(String tableName) {
    String key = SCHEMA_VERSION_PREFIX + tableName;
    String value = snapshot.get(key);
    return value != null ? Long.parseLong(value) : null;
}

/**
 * 从 snapshot 获取列位置映射
 */
private Map<String, Integer> getColumnOrdinalPositions(String tableName) throws Exception {
    String key = SCHEMA_ORDINAL_PREFIX + tableName;
    String json = snapshot.get(key);
    if (json == null || json.isEmpty()) {
        return new HashMap<>();
    }
    return JsonUtil.jsonToObj(json, Map.class);
}

/**
 * 查询表的当前结构（返回 MetaInfo 和列位置映射）
 */
private MetaInfo queryTableMetaInfo(String tableName, Map<String, Integer> ordinalPositions) throws Exception {
    // 查询列信息
    String sql = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, " +
                 "NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION " +
                 "FROM INFORMATION_SCHEMA.COLUMNS " +
                 "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
                 "ORDER BY ORDINAL_POSITION";
    
    List<Field> columns = queryAndMapList(sql, statement -> {
        statement.setString(1, schema);
        statement.setString(2, tableName);
    }, rs -> {
        List<Field> cols = new ArrayList<>();
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            String dataType = rs.getString("DATA_TYPE");
            Integer maxLength = rs.getObject("CHARACTER_MAXIMUM_LENGTH") != null 
                ? rs.getInt("CHARACTER_MAXIMUM_LENGTH") : null;
            Integer precision = rs.getObject("NUMERIC_PRECISION") != null 
                ? rs.getInt("NUMERIC_PRECISION") : null;
            Integer scale = rs.getObject("NUMERIC_SCALE") != null 
                ? rs.getInt("NUMERIC_SCALE") : null;
            Boolean nullable = "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
            String defaultValue = rs.getString("COLUMN_DEFAULT");
            Integer ordinalPosition = rs.getInt("ORDINAL_POSITION");
            
            // 创建 Field 对象
            Field col = new Field();
            col.setName(columnName);
            col.setTypeName(dataType);
            col.setColumnSize(maxLength != null ? maxLength : (precision != null ? precision : 0));
            col.setRatio(scale != null ? scale : 0);
            col.setNullable(nullable);
            col.setDefaultValue(defaultValue);
            
            // 保存列位置（Field 类中没有此字段，需要单独存储到 snapshot）
            ordinalPositions.put(columnName, ordinalPosition);
            
            cols.add(col);
        }
        return cols;
    });
    
    // 查询主键（用于设置 Field.isPk()）
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
    
    // 设置主键标识
    for (Field col : columns) {
        col.setPk(primaryKeys.contains(col.getName()));
    }
    
    // 创建 MetaInfo 对象
    MetaInfo metaInfo = new MetaInfo();
    metaInfo.setTableType("TABLE");
    metaInfo.setColumn(columns);
    
    return metaInfo;
}

/**
 * 从 ResultSetMetaData 计算表结构哈希值（用于 DML 同步过程中的快速检测）
 */
private String calculateSchemaHashFromMetaData(ResultSetMetaData metaData) throws SQLException {
    StringBuilder hashInput = new StringBuilder();
    int columnCount = metaData.getColumnCount();
    for (int i = 1; i <= columnCount; i++) {
        // 跳过 Change Tracking 系统列（SYS_CHANGE_VERSION, SYS_CHANGE_OPERATION, SYS_CHANGE_COLUMNS）
        if (i <= 3) continue;
        hashInput.append(metaData.getColumnName(i))
                .append("|").append(metaData.getColumnTypeName(i))
                .append("|").append(metaData.getColumnDisplaySize(i))
                .append("|").append(metaData.getPrecision(i))
                .append("|").append(metaData.getScale(i))
                .append("|").append(metaData.isNullable(i))
                .append("|");
    }
    return DigestUtils.md5Hex(hashInput.toString());
}

/**
 * 从 MetaInfo 计算表结构哈希值（用于完整比对前的快速检测）
 */
private String calculateSchemaHashFromMetaInfo(MetaInfo metaInfo) {
    if (metaInfo == null || metaInfo.getColumn() == null) {
        return "";
    }
    StringBuilder hashInput = new StringBuilder();
    for (Field field : metaInfo.getColumn()) {
        hashInput.append(field.getName())
                .append("|").append(field.getTypeName())
                .append("|").append(field.getColumnSize())
                .append("|").append(field.getRatio())
                .append("|").append(field.getNullable())
                .append("|");
    }
    return DigestUtils.md5Hex(hashInput.toString());
}
```

### 5.2 DDL 事件与 DML 事件合并

```java
/**
 * 合并 DDL 和 DML 事件（按版本号排序）
 */
private void mergeAndProcessEvents(Long stopVersion) throws Exception {
    // 1. 查询 DML 变更（在查询过程中会检测 DDL 变更）
    List<CTEvent> dmlEvents = pullDMLChanges(stopVersion);
    
    // 2. 从 DDL 队列中取出所有待处理的 DDL 事件
    List<DDLEvent> ddlEvents = new ArrayList<>();
    DDLEvent ddlEvent;
    while ((ddlEvent = ddlEventQueue.poll()) != null) {
        if (ddlEvent.getVersion() <= stopVersion) {
            ddlEvents.add(ddlEvent);
        } else {
            // 版本号超过 stopVersion，放回队列
            ddlEventQueue.offer(ddlEvent);
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
- DDL 变更通过在 DML 查询时检测哈希值变化来发现，使用 Change Tracking 版本号进行排序
- 确保 DDL 和 DML 变更可以按版本号合并排序
- 表结构快照保存到 `snapshot`，支持断点续传
- 首次检测时只保存快照，不生成 DDL 事件
- 不需要定期轮询，在 DML 查询时即可检测到 DDL 变更

### 5.3 DDL 检测实现方案

**实现策略**：
1. **哈希值检测**：在 DML 查询过程中，从 `ResultSetMetaData` 获取表结构信息，计算哈希值并比对
2. **完整比对**：哈希值变化时，立即查询 `INFORMATION_SCHEMA` 进行完整比对，生成 DDL 事件

**关键实现**：

```java
// 在 pullDMLChanges 中检测哈希值
private List<CTEvent> pullDMLChanges(String tableName, Long startVersion, Long stopVersion) throws Exception {
    // ... 构建查询 SQL ...
    
    return queryAndMapList(sql, statement -> {
        statement.setLong(1, startVersion);
        statement.setLong(2, startVersion);
        statement.setLong(3, stopVersion);
    }, rs -> {
        // 在第一次获取结果时检测表结构
        ResultSetMetaData metaData = rs.getMetaData();
        detectDDLChangesInDMLQuery(tableName, metaData);
        
        List<CTEvent> events = new ArrayList<>();
        while (rs.next()) {
            // ... 处理 DML 事件 ...
        }
        return events;
    });
}
```

**为什么不需要轮询？**

1. **精确性**：哈希值变化时，可以在 DML 查询时立即查询 `INFORMATION_SCHEMA`，不需要轮询
2. **兜底机制**：如果表结构变了但没有 DML 变更，没有数据需要同步，表结构变化不会影响数据同步；当有 DML 变更时，会在查询时检测到结构变化
3. **完整性**：只需要检测影响数据结构的 DDL（增删改列），这些都可以在 DML 查询时检测到

**sp_rename 支持**：
- 通过完整比对可以检测到列名变化
- 通过比对列属性（除列名外）是否相同来判断是 RENAME 还是 DROP+ADD

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
    "SELECT is_tracked_by_cdc FROM sys.tables WHERE name = '%s'";  // 注意：字段名 is_tracked_by_cdc 实际用于 Change Tracking（不是 CDC）
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
        // 在第一次获取结果时检测表结构（DDL 变更）
        ResultSetMetaData metaData = rs.getMetaData();
        detectDDLChangesInDMLQuery(tableName, metaData);
        
        List<CTEvent> events = new ArrayList<>();
        while (rs.next()) {
            Long version = rs.getLong(1);
            String operation = rs.getString(2);  // 'I', 'U', 'D'
            byte[] columnsUpdated = rs.getBytes(3);  // 更新的列（二进制）
            
            // 构建行数据
            List<Object> row = new ArrayList<>();
            for (int i = 4; i <= metaData.getColumnCount(); i++) {
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
 * DDL 变更在 DML 查询时检测，这里从队列中获取
 */
private List<DDLEvent> pullDDLChanges(Long startVersion, Long stopVersion) throws Exception {
    List<DDLEvent> events = new ArrayList<>();
    
    // 从队列中取出所有在版本范围内的 DDL 事件
    DDLEvent ddlEvent;
    while ((ddlEvent = ddlEventQueue.poll()) != null) {
        if (ddlEvent.getVersion() > startVersion && ddlEvent.getVersion() <= stopVersion) {
            // 检查表名是否匹配
            if (filterTable.contains(ddlEvent.getTableName())) {
                events.add(ddlEvent);
            }
        } else if (ddlEvent.getVersion() > stopVersion) {
            // 版本号超过范围，放回队列
            ddlEventQueue.offer(ddlEvent);
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

## 九、关键注意事项

### 9.1 主键要求

- Change Tracking 要求表必须有主键
- 复合主键需要特殊处理 JOIN 条件

### 9.2 DELETE 操作处理

- DELETE 操作无法 JOIN 原表获取数据
- 方案：只发送主键信息，由上层应用处理

### 9.3 版本号精度

- Change Tracking 版本号是单调递增的整数
- 无法像 LSN 那样精确反映事务时间点
- 对于同一事务内的多个变更，版本号可能相同

### 9.4 DDL 检测机制

- DDL 检测在 DML 查询时触发，通过 `ResultSetMetaData` 计算哈希值进行快速检测
- 哈希值变化时，立即查询 `INFORMATION_SCHEMA` 进行完整比对
- 表结构快照保存到 `snapshot`，占用一定存储空间
- 首次检测时只保存快照，不生成 DDL 事件
- 某些复杂 DDL（如索引变更、约束变更）可能无法检测
- 不需要定期轮询，减少数据库负载

### 9.5 变更保留时间

- `CHANGE_RETENTION` 设置建议 2-7 天
- 如果同步延迟超过保留时间，需要全量同步

## 十、性能优化

- 使用 `TRACK_COLUMNS_UPDATED = ON` 优化 UPDATE 查询
- DDL 检测在 DML 查询时触发，利用 `ResultSetMetaData` 进行哈希值检测，零额外开销
- 哈希值变化时才查询 `INFORMATION_SCHEMA`，避免不必要的查询
- 不需要定期轮询表结构，减少数据库负载

## 十一、错误处理

### 11.1 版本号丢失

- 如果 `lastVersion` 丢失，从当前版本号开始（可能导致数据重复）
- 建议：记录版本号到快照，支持手动恢复

### 11.2 DDL 检测失败

- DDL 检测失败不影响 DML 同步，但会导致 DDL 变更丢失
- 建议：监控检测器状态，记录失败日志，支持手动触发检测

### 11.3 表结构快照损坏

- 如果表结构快照损坏或丢失，首次检测时会重新生成
- 建议：定期备份 `snapshot`，支持手动恢复

### 11.4 变更保留时间过期

- 如果同步延迟超过 `CHANGE_RETENTION`，变更会被清理
- 建议：触发全量同步，或增加保留时间


