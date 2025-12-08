# 表数据统计优化方案评估

## 背景

当前系统使用 `SELECT COUNT(*)` 或 `SELECT COUNT(1)` 来统计表数据量，对于大表来说性能较差。由于统计值不需要特别准确，可以考虑使用数据库元数据表来获取近似值，以提升性能。

## 各数据库替代方案

### MySQL/MariaDB

**方案：** 查询 `information_schema.tables` 表的 `table_rows` 列

**SQL：**
```sql
SELECT table_rows 
FROM information_schema.tables 
WHERE table_schema = DATABASE() 
  AND table_name = 'table_name'
```

**注意事项：**
- `table_rows` 是近似值，对于 InnoDB 表来说可能不够准确
- 需要定期执行 `ANALYZE TABLE` 来更新统计信息
- 如果表有过滤条件，无法使用此方法

**性能：** 极快，毫秒级

### SQL Server

**方案：** 查询 `sys.partitions` 和 `sys.tables` 的 `rows` 列

**SQL：**
```sql
SELECT p.rows 
FROM sys.tables t 
INNER JOIN sys.partitions p ON t.object_id = p.object_id 
WHERE t.name = 'table_name' 
  AND t.schema_id = SCHEMA_ID('schema_name')
  AND p.index_id IN (0, 1)  -- 0=堆, 1=聚集索引
```

**注意事项：**
- `rows` 是近似值，可能不够准确
- 需要定期更新统计信息（`UPDATE STATISTICS`）
- 如果表有过滤条件，无法使用此方法

**性能：** 极快，毫秒级

### PostgreSQL

**方案：** 查询 `pg_class` 表的 `reltuples` 列

**SQL：**
```sql
SELECT reltuples::BIGINT AS row_count
FROM pg_class
WHERE relname = 'table_name'
  AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'schema_name')
```

**注意事项：**
- `reltuples` 是近似值，由 VACUUM 和 ANALYZE 更新
- 需要定期执行 `ANALYZE` 来更新统计信息
- 如果表有过滤条件，无法使用此方法

**性能：** 极快，毫秒级

### Oracle

**方案：** 查询 `user_tables` 或 `all_tables` 的 `num_rows` 列

**SQL：**
```sql
SELECT num_rows 
FROM user_tables 
WHERE table_name = 'TABLE_NAME'
```

或者使用 `all_tables`（需要指定 schema）：
```sql
SELECT num_rows 
FROM all_tables 
WHERE owner = 'SCHEMA_NAME' 
  AND table_name = 'TABLE_NAME'
```

**注意事项：**
- `num_rows` 是近似值，由统计信息收集器更新
- 需要定期执行 `ANALYZE TABLE` 或 `DBMS_STATS.GATHER_TABLE_STATS` 来更新统计信息
- 如果表有过滤条件，无法使用此方法

**性能：** 极快，毫秒级

### SQLite

**方案：** SQLite 没有专门的元数据表存储行数，但可以使用 `sqlite_stat1` 表（如果存在）

**SQL：**
```sql
SELECT stat 
FROM sqlite_stat1 
WHERE tbl = 'table_name' 
  AND idx IS NULL
```

**注意事项：**
- SQLite 的统计信息需要手动创建和维护
- 如果没有统计信息，只能使用 `SELECT COUNT(*)`
- 对于 SQLite，建议保持使用 `COUNT(*)`，因为 SQLite 通常用于小表

**性能：** 如果统计信息存在则快，否则需要实际扫描

## 实现策略

### 1. 条件判断

只有在以下条件都满足时，才使用元数据查询：
- 没有 WHERE 过滤条件（`queryFilter` 为空）
- 数据库类型支持元数据查询
- 用户配置允许使用近似值（可选配置项）

### 2. 降级策略

如果元数据查询失败或返回 null，自动降级到 `SELECT COUNT(*)`

### 3. 配置选项

可以添加配置项控制是否启用此优化：
- `dbsyncer.connector.useMetadataCount=true`（默认 true）

## 性能对比

| 数据库 | 方法 | 大表（千万级） | 小表（万级） |
|--------|------|---------------|-------------|
| MySQL | COUNT(*) | 10-60秒 | <1秒 |
| MySQL | table_rows | <10ms | <10ms |
| SQL Server | COUNT(*) | 30-120秒 | <1秒 |
| SQL Server | sys.partitions | <10ms | <10ms |
| PostgreSQL | COUNT(*) | 20-80秒 | <1秒 |
| PostgreSQL | pg_class | <10ms | <10ms |
| Oracle | COUNT(*) | 30-120秒 | <1秒 |
| Oracle | user_tables | <10ms | <10ms |

## 注意事项

1. **准确性：** 元数据统计值是近似值，可能比实际值偏差 10-30%
2. **更新频率：** 需要数据库定期更新统计信息，否则偏差会更大
3. **过滤条件：** 如果有 WHERE 条件，必须使用实际 COUNT 查询
4. **兼容性：** 需要确保所有数据库版本都支持相应的元数据查询

## 推荐实现

建议在 `AbstractDatabaseConnector.getCount()` 方法中实现此优化，根据数据库类型和查询条件智能选择使用元数据查询还是实际 COUNT 查询。
