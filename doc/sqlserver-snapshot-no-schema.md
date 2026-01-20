# SQL Server CT 增量同步：Snapshot Schema 信息不持久化改造方案

## 一、问题分析

### 1.1 当前问题

在 SQL Server CT（Change Tracking）增量同步过程中，当 snapshot 变化时会持久化 meta 信息。但快照中的 schema 相关信息较大，且每个表都有一套，比较影响 IO 性能。

**当前持久化的 Schema 相关信息**：
- `schema_snapshot_<tableName>`：完整的表结构 JSON（最大，包含所有列的详细信息）
- `schema_ordinal_<tableName>`：列位置映射 JSON（中等大小）
- `schema_hash_<tableName>`：表结构哈希值（很小，32 字符）
- `schema_version_<tableName>`：快照版本号（很小）
- `schema_time_<tableName>`：快照时间戳（很小）

**示例数据大小**：
- `schema_snapshot_TestTable`：约 2-5KB（20 列的表）
- `schema_ordinal_TestTable`：约 200-500 字节
- `schema_hash_TestTable`：32 字节
- `schema_version_TestTable`：约 10 字节
- `schema_time_TestTable`：约 13 字节

**影响**：
- 每个表每次 DDL 变更都会触发持久化，包含完整的 schema 信息
- 多表场景下，IO 压力显著增加
- Schema 信息在持久化数据中占比最大（约 80-90%）

### 1.2 Schema 信息的使用场景

**当前使用方式**：
1. **DDL 变更检测**：通过比较哈希值快速检测是否有 DDL 变更
2. **详细比对**：当哈希值变化时，加载旧的 schema 信息与新的 schema 信息进行详细比对，生成具体的 DDL 事件（ADD_COLUMN、DROP_COLUMN、ALTER_COLUMN、RENAME_COLUMN 等）

**关键代码位置**：
- `SqlServerCTListener.detectDDLChangesFromSchemaInfoJson()`：检测 DDL 变更
- `SqlServerCTListener.saveTableSchemaSnapshotFromJson()`：保存 schema 快照
- `SqlServerCTListener.loadTableSchemaSnapshot()`：加载 schema 快照
- `SqlServerCTListener.compareTableSchema()`：比对表结构差异

## 二、改造方案

### 2.1 核心思路

**完全不持久化 Schema 相关信息**：
- ❌ **移除**：`schema_hash_<tableName>`（不持久化，只在内存中维护）
- ❌ **移除**：`schema_version_<tableName>`（不持久化）
- ❌ **移除**：`schema_time_<tableName>`（不持久化）
- ❌ **移除**：`schema_snapshot_<tableName>`（不持久化）
- ❌ **移除**：`schema_ordinal_<tableName>`（不持久化）

**Schema 信息处理策略**：
1. **完全内存化**：所有 Schema 相关信息（包括哈希值、版本号、时间戳、完整 schema、列位置映射）都只在内存中维护，不持久化到 snapshot
2. **首次检测**：首次检测时，如果内存缓存中没有旧的 schema，直接更新内存缓存，不进行 DDL 变更检测（因为无法知道是否有变更）
3. **后续检测**：后续检测时，从内存缓存中获取旧的 schema 和哈希值，与新的 schema 和哈希值进行比对，检测 DDL 变更
4. **重启恢复**：重启后，内存缓存丢失，首次检测时直接更新内存缓存，不进行 DDL 变更检测；后续检测时才能进行 DDL 变更检测

### 2.2 详细设计

#### 2.2.1 内存缓存 Schema 信息

**新增内存缓存字段位置**：
在 `SqlServerCTListener` 类中，在现有缓存字段附近（第 73-76 行，`primaryKeysCache` 和 `tableColumnCountCache` 之后）添加以下字段：

```java
// 主键信息缓存（表名 -> 主键列表）
private final Map<String, List<String>> primaryKeysCache = new HashMap<>();
// 表列数缓存（表名 -> 列数），避免重复查询 INFORMATION_SCHEMA
private final Map<String, Integer> tableColumnCountCache = new HashMap<>();

// ✅ 新增：Schema 信息内存缓存（表名 -> MetaInfo）
private final Map<String, MetaInfo> schemaCache = new ConcurrentHashMap<>();

// ✅ 新增：列位置映射内存缓存（表名 -> Map<列名, 位置>）
private final Map<String, Map<String, Integer>> ordinalPositionsCache = new ConcurrentHashMap<>();

// ✅ 新增：Schema 哈希值内存缓存（表名 -> 哈希值）
private final Map<String, String> schemaHashCache = new ConcurrentHashMap<>();

// 注意：不需要为每个表单独记录版本号
// - DDL 变更检测时使用的是 getCurrentVersion() 获取当前的 Change Tracking 版本号
// - 如果只是用于调试/监控，Change Tracking 的全局版本号（VERSION_POSITION）已经足够了
```

**代码位置**：
- 文件：`dbsyncer-connector/dbsyncer-connector-sqlserver/src/main/java/org/dbsyncer/connector/sqlserver/ct/SqlServerCTListener.java`
- 位置：第 76 行之后（`tableColumnCountCache` 字段定义之后）

**缓存更新时机**：
- 首次检测时：解析 schemaInfoJson 并缓存所有信息（schema、哈希值、列位置映射）
- DDL 变更检测后：更新缓存为最新的 schema 信息

**说明**：
- 不需要 `schemaVersionCache`：DDL 变更检测时使用的是 `getCurrentVersion()` 获取当前的 Change Tracking 版本号，不需要为每个表单独记录版本号
- 如果只是用于调试/监控，Change Tracking 的全局版本号（`VERSION_POSITION`）已经足够了

#### 2.2.2 修改保存方法

**修改 `saveTableSchemaSnapshotFromJson()` 方法**：

```java
private void saveTableSchemaSnapshotFromJson(String tableName, String schemaInfoJson, Long version,
                                             String hash, Map<String, Integer> ordinalPositions) throws Exception {
    // ❌ 完全不持久化任何 schema 相关信息到 snapshot
    // String schemaKey = SCHEMA_SNAPSHOT_PREFIX + tableName;
    // snapshot.put(schemaKey, schemaInfoJson);
    // String hashKey = SCHEMA_HASH_PREFIX + tableName;
    // snapshot.put(hashKey, hash);
    // String versionKey = SCHEMA_VERSION_PREFIX + tableName;
    // snapshot.put(versionKey, String.valueOf(version));
    // String timeKey = SCHEMA_TIME_PREFIX + tableName;
    // snapshot.put(timeKey, String.valueOf(System.currentTimeMillis()));
    // String ordinalKey = SCHEMA_ORDINAL_PREFIX + tableName;
    // String ordinalJson = JsonUtil.objToJson(ordinalPositions);
    // snapshot.put(ordinalKey, ordinalJson);
    
    // ✅ 只更新内存缓存（所有信息都在内存中维护）
    MetaInfo metaInfo = parseSchemaInfoJson(tableName, schemaInfoJson);
    schemaCache.put(tableName, metaInfo);
    ordinalPositionsCache.put(tableName, ordinalPositions);
    schemaHashCache.put(tableName, hash);
    // 注意：不需要缓存版本号，DDL 变更检测时使用 getCurrentVersion() 获取当前的 Change Tracking 版本号
    
    // 注意：不再调用 super.refreshEvent(null)，因为不需要持久化 schema 信息
    // Change Tracking 版本号的持久化由其他逻辑处理（如 VERSION_POSITION）
}
```

#### 2.2.3 修改加载方法

**修改 `loadTableSchemaSnapshot()` 方法**：

```java
private MetaInfo loadTableSchemaSnapshot(String tableName) throws Exception {
    // ✅ 只从内存缓存加载
    return schemaCache.get(tableName);
}
```

**修改 `getSchemaHash()` 方法**：

```java
private String getSchemaHash(String tableName) {
    // ✅ 只从内存缓存加载
    return schemaHashCache.get(tableName);
}
```

**修改 `getColumnOrdinalPositions()` 方法**：

```java
private Map<String, Integer> getColumnOrdinalPositions(String tableName) throws Exception {
    // ✅ 只从内存缓存加载
    Map<String, Integer> cachedOrdinalPositions = ordinalPositionsCache.get(tableName);
    return cachedOrdinalPositions != null ? cachedOrdinalPositions : new HashMap<>();
}
```

#### 2.2.4 修改 DDL 检测逻辑

**修改 `detectDDLChangesFromSchemaInfoJson()` 方法**：

```java
private void detectDDLChangesFromSchemaInfoJson(String tableName, String schemaInfoJson) throws Exception {
    if (schemaInfoJson == null || schemaInfoJson.trim().isEmpty()) {
        logger.warn("表 {} 的表结构信息 JSON 为空，跳过 DDL 检测", tableName);
        return;
    }

    // 1. 直接基于 JSON 字符串计算哈希值（快速，避免解析 JSON 的开销）
    String currentHash = DigestUtils.md5Hex(schemaInfoJson);
    String lastHash = getSchemaHash(tableName);  // ✅ 从内存缓存获取

    if (lastHash == null) {
        // 首次检测（内存缓存中没有旧的哈希值），直接更新内存缓存，不进行 DDL 变更检测
        // 因为无法知道是否有变更（没有旧的 schema 信息进行比对）
        Long currentVersion = getCurrentVersion();
        Map<String, Integer> ordinalPositions = extractOrdinalPositions(schemaInfoJson);
        
        // ✅ 只更新内存缓存（不持久化）
        saveTableSchemaSnapshotFromJson(tableName, schemaInfoJson, currentVersion, currentHash, ordinalPositions);

        // 首次检测时也更新缓存（如果缓存为空）
        if (primaryKeysCache.get(tableName) == null || tableColumnCountCache.get(tableName) == null) {
            MetaInfo currentMetaInfo = parseSchemaInfoJson(tableName, schemaInfoJson);
            List<String> currentPrimaryKeys = getPrimaryKeysFromMetaInfo(currentMetaInfo);
            int currentColumnCount = ordinalPositions.size();
            primaryKeysCache.put(tableName, currentPrimaryKeys);
            tableColumnCountCache.put(tableName, currentColumnCount);
            logger.debug("首次检测表 {} 时更新缓存：主键={}, 列数={}", tableName, currentPrimaryKeys, currentColumnCount);
        }
        logger.debug("表 {} 首次检测，更新内存缓存，不进行 DDL 变更检测", tableName);
        return;
    }

    if (lastHash.equals(currentHash)) {
        return; // 哈希值未变化，无 DDL 变更（避免解析 JSON 的开销）
    }

    // 2. 哈希值变化，解析 JSON 构建 MetaInfo 进行详细比对
    MetaInfo currentMetaInfo = parseSchemaInfoJson(tableName, schemaInfoJson);
    Long currentVersion = getCurrentVersion();
    MetaInfo lastMetaInfo = loadTableSchemaSnapshot(tableName);  // ✅ 从内存缓存加载

    if (lastMetaInfo != null) {
        // ✅ 内存缓存中有旧的 schema，进行详细比对
        List<String> lastPrimaryKeys = getPrimaryKeysFromMetaInfo(lastMetaInfo);
        List<String> currentPrimaryKeys = getPrimaryKeysFromMetaInfo(currentMetaInfo);
        Map<String, Integer> lastOrdinalPositions = getColumnOrdinalPositions(tableName);  // ✅ 从内存缓存加载
        Map<String, Integer> currentOrdinalPositions = extractOrdinalPositions(schemaInfoJson);

        // 3. 比对差异
        logger.info("开始比对表 {} 的 DDL 变更: 旧列数={}, 新列数={}", 
                tableName, lastMetaInfo.getColumn().size(), currentMetaInfo.getColumn().size());
        List<DDLChange> changes = compareTableSchema(tableName, lastMetaInfo, currentMetaInfo,
                lastPrimaryKeys, currentPrimaryKeys,
                lastOrdinalPositions, currentOrdinalPositions);
        logger.info("比对完成，检测到 {} 个 DDL 变更", changes.size());

        // 4. 如果有变更，生成 DDL 事件
        if (!changes.isEmpty()) {
            for (DDLChange change : changes) {
                CTDDLEvent ddlEvent = new CTDDLEvent(
                        tableName,
                        change.getDdlCommand(),
                        currentVersion,
                        new Date()
                );
                ddlEventQueue.offer(ddlEvent);
            }

            // 5. 更新内存缓存（不持久化）
            Map<String, Integer> ordinalPositions = extractOrdinalPositions(schemaInfoJson);
            saveTableSchemaSnapshotFromJson(tableName, schemaInfoJson, currentVersion, currentHash, ordinalPositions);

            // 6. 更新缓存：DDL 变更可能影响主键或列数
            boolean needUpdateCache = false;
            for (DDLChange change : changes) {
                DDLChangeType changeType = change.getChangeType();
                if (changeType == DDLChangeType.ADD_COLUMN ||
                        changeType == DDLChangeType.DROP_COLUMN ||
                        changeType == DDLChangeType.ALTER_PRIMARY_KEY) {
                    needUpdateCache = true;
                    break;
                }
            }

            if (needUpdateCache) {
                primaryKeysCache.put(tableName, currentPrimaryKeys);
                int currentColumnCount = extractOrdinalPositions(schemaInfoJson).size();
                tableColumnCountCache.put(tableName, currentColumnCount);
                logger.debug("已更新表 {} 的缓存：主键={}, 列数={}", tableName, currentPrimaryKeys, currentColumnCount);
            }

            logger.info("检测到表 {} 的 DDL 变更，共 {} 个变更", tableName, changes.size());
        } else {
            // 即使没有检测到变更，也更新内存缓存（可能是精度/尺寸变化导致哈希变化但无法检测）
            Map<String, Integer> ordinalPositions = extractOrdinalPositions(schemaInfoJson);
            saveTableSchemaSnapshotFromJson(tableName, schemaInfoJson, currentVersion, currentHash, ordinalPositions);
        }
    } else {
        // ✅ 内存缓存中没有旧的 schema（重启后首次检测或缓存丢失）
        // 无法进行详细比对，直接更新内存缓存，不发送 DDL 事件
        // 因为无法知道是否有变更（没有旧的 schema 信息进行比对）
        logger.warn("表 {} 的 DDL 变更检测：内存缓存中没有旧的 schema，无法进行详细比对，直接更新内存缓存", tableName);
        
        // 更新内存缓存
        Map<String, Integer> ordinalPositions = extractOrdinalPositions(schemaInfoJson);
        saveTableSchemaSnapshotFromJson(tableName, schemaInfoJson, currentVersion, currentHash, ordinalPositions);
        
        logger.warn("表 {} 的 DDL 变更检测：重启后首次检测，已更新内存缓存，后续检测时才能进行 DDL 变更检测", tableName);
    }
}
```

#### 2.2.5 启动时初始化 Schema 缓存

**新增初始化方法**：

在 `start()` 方法中，在 `readLastVersion()` 之后、启动 worker 之前，添加初始化 schema 缓存的调用：

```java
@Override
public void start() throws Exception {
    try {
        connectLock.lock();
        if (connected) {
            logger.error("SqlServerCTListener is already started");
            return;
        }
        connected = true;
        connect();
        readTables();
        Assert.notEmpty(tables, "No tables available");

        enableDBChangeTracking();
        enableTableChangeTracking();
        readLastVersion();
        
        // ✅ 新增：启动时初始化所有表的 schema 缓存
        initializeSchemaCache();

        worker = new Worker();
        worker.setName(new StringBuilder("ct-parser-").append(serverName).append("_").append(worker.hashCode()).toString());
        worker.setDaemon(false);
        worker.start();
        VersionPuller.addExtractor(metaId, this);
    } catch (Exception e) {
        close();
        logger.error("启动失败: {}", e.getMessage(), e);
        throw new SqlServerException(e);
    } finally {
        connectLock.unlock();
    }
}
```

**新增 `initializeSchemaCache()` 方法**：

```java
/**
 * 启动时初始化所有表的 schema 缓存
 * 预先加载所有表的 schema 信息到内存缓存中，确保重启后首次检测时就能进行 DDL 变更检测
 */
private void initializeSchemaCache() throws Exception {
    if (CollectionUtils.isEmpty(tables)) {
        return;
    }
    
    logger.info("开始初始化 schema 缓存，共 {} 个表", tables.size());
    int successCount = 0;
    int failCount = 0;
    
    for (String tableName : tables) {
        try {
            // 查询表结构信息
            String schemaInfoSubquery = sqlTemplate.buildGetTableSchemaInfoSubquery(schema, tableName);
            String querySql = String.format("SELECT %s AS schema_info", schemaInfoSubquery);
            
            String schemaInfoJson = queryAndMap(querySql, rs -> {
                if (rs.next()) {
                    return rs.getString("schema_info");
                }
                return null;
            });
            
            if (schemaInfoJson != null && !schemaInfoJson.trim().isEmpty()) {
                // 计算哈希值
                String hash = DigestUtils.md5Hex(schemaInfoJson);
                Long currentVersion = getCurrentVersion();
                Map<String, Integer> ordinalPositions = extractOrdinalPositions(schemaInfoJson);
                
                // 更新内存缓存
                saveTableSchemaSnapshotFromJson(tableName, schemaInfoJson, currentVersion, hash, ordinalPositions);
                
                // 同时更新主键和列数缓存（如果缓存为空）
                if (primaryKeysCache.get(tableName) == null || tableColumnCountCache.get(tableName) == null) {
                    MetaInfo metaInfo = parseSchemaInfoJson(tableName, schemaInfoJson);
                    List<String> primaryKeys = getPrimaryKeysFromMetaInfo(metaInfo);
                    int columnCount = ordinalPositions.size();
                    primaryKeysCache.put(tableName, primaryKeys);
                    tableColumnCountCache.put(tableName, columnCount);
                }
                
                successCount++;
                logger.debug("表 {} 的 schema 缓存初始化成功", tableName);
            } else {
                logger.warn("表 {} 的表结构信息为空，跳过初始化", tableName);
                failCount++;
            }
        } catch (Exception e) {
            logger.error("表 {} 的 schema 缓存初始化失败: {}", tableName, e.getMessage(), e);
            failCount++;
        }
    }
    
    logger.info("Schema 缓存初始化完成：成功 {} 个，失败 {} 个", successCount, failCount);
}
```

**初始化时机**：
- 在 `start()` 方法中，在 `readLastVersion()` 之后、启动 worker 之前调用
- 确保在开始处理增量数据之前，所有表的 schema 缓存都已建立

**初始化内容**：
- 查询每个表的 schema 信息（通过 `buildGetTableSchemaInfoSubquery`）
- 计算哈希值并缓存
- 解析并缓存完整的 schema 信息（MetaInfo）
- 缓存列位置映射（ordinalPositions）
- 同时更新主键和列数缓存（如果缓存为空）

**优势**：
- 重启后首次检测时就能进行 DDL 变更检测（因为内存缓存已建立）
- 避免了重启后首次检测时无法检测 DDL 变更的问题
- 初始化过程在启动时完成，不影响后续的增量同步性能

## 三、改造影响分析

### 3.1 性能优化

**IO 性能提升**：
- **完全消除 Schema 相关持久化**：每个表每次 DDL 变更，不再持久化任何 Schema 相关信息（从约 2-5KB 减少到 0 字节）
- **减少 IO 次数**：Schema 信息完全不触发持久化，只有 Change Tracking 版本号变化时才触发持久化
- **多表场景优化**：多表场景下，IO 压力显著降低（完全消除 Schema 相关的持久化 IO）

**内存使用**：
- **内存缓存**：Schema 信息只在内存中缓存，重启后丢失
- **内存占用**：每个表的 schema 信息约 2-5KB，100 个表约 200-500KB，内存占用可接受

### 3.2 功能影响

**正常场景**（无重启）：
- ✅ **功能完全正常**：Schema 信息在内存中缓存，DDL 变更检测和详细比对功能完全正常
- ✅ **性能提升**：完全消除 Schema 相关的持久化 IO，性能显著提升

**重启场景**：
- ✅ **启动时初始化**：在 `start()` 方法中，启动 worker 之前，预先加载所有表的 schema 信息到内存缓存中
- ✅ **初始化后即可检测**：初始化完成后，内存缓存已建立，首次检测时就能进行 DDL 变更检测
- ⚠️ **初始化期间限制**：初始化过程中（启动时），如果发生 DDL 变更可能无法被检测到（但初始化时间很短，通常几秒内完成，影响有限）
- ✅ **初始化性能**：初始化过程在启动时完成，不影响后续的增量同步性能

**DDL 变更检测准确性**：
- ✅ **哈希值检测**：哈希值检测功能完全正常（在内存中维护），可以快速检测是否有 DDL 变更
- ✅ **详细比对**：正常场景下详细比对完全正常；重启后首次检测时跳过 DDL 变更检测，后续检测时恢复正常

### 3.3 兼容性

**向后兼容**：
- ✅ **向后兼容**：旧的 snapshot 中包含所有 schema 相关信息，新代码会忽略这些字段（不再读取）
- ✅ **平滑升级**：升级后，旧的 schema 信息会被忽略，首次检测时会重新建立内存缓存

**数据迁移**：
- ✅ **无需迁移**：旧的 snapshot 数据可以保留，新代码会自动忽略所有 schema 相关字段
- ✅ **自动清理**：旧的 schema 信息会被忽略，不会影响新代码的运行

## 四、实施步骤

1. **修改 `SqlServerCTListener.java`**：
   - 新增内存缓存字段：`schemaCache`、`ordinalPositionsCache`、`schemaHashCache`（不需要 `schemaVersionCache`）
   - 修改 `start()` 方法：在 `readLastVersion()` 之后、启动 worker 之前，添加 `initializeSchemaCache()` 调用
   - 新增 `initializeSchemaCache()` 方法：启动时初始化所有表的 schema 缓存
   - 修改 `saveTableSchemaSnapshotFromJson()` 方法：完全不持久化任何 schema 相关信息，只更新内存缓存
   - 修改 `loadTableSchemaSnapshot()` 方法：只从内存缓存加载
   - 修改 `getSchemaHash()` 方法：只从内存缓存加载
   - 修改 `getColumnOrdinalPositions()` 方法：只从内存缓存加载
   - 修改 `detectDDLChangesFromSchemaInfoJson()` 方法：处理内存缓存中没有旧 schema 的情况（正常情况下不会发生，因为启动时已初始化）

## 五、总结

### 5.1 改造收益

1. **IO 性能提升**：完全消除 Schema 相关的持久化 IO，IO 性能显著提升
2. **功能保持**：正常场景下功能完全正常，重启场景下有降级处理（首次检测时跳过 DDL 变更检测）
3. **向后兼容**：无需数据迁移，平滑升级

### 5.2 注意事项

1. **启动初始化**：启动时会初始化所有表的 schema 缓存，初始化时间取决于表的数量（通常几秒内完成）
2. **内存使用**：所有 Schema 相关信息都在内存中缓存，重启后丢失，启动时重新建立缓存
3. **初始化期间**：初始化过程中（启动时），如果发生 DDL 变更可能无法被检测到（但初始化时间很短，影响有限）
4. **监控告警**：需要监控 DDL 变更检测的准确性，及时发现异常情况
5. **初始化失败处理**：如果某个表的 schema 缓存初始化失败，会记录日志但不影响其他表的初始化，该表会在首次检测时自动初始化

