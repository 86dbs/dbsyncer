# SQL Server DDL 同步功能实施文档

## 一、概述

在现有的 SQL Server 增量数据同步机制基础上，增加 DDL（数据定义语言）变更的捕获和同步功能。

**目标**：
- 捕获 SQL Server 表的 DDL 变更（ALTER TABLE）
- 保持 DDL 和 DML 事件的时序一致性
- 支持断点续传（DDL LSN 位置持久化）

**技术要点**：
- SQL Server CDC 的 `cdc.ddl_history` 表记录所有 DDL 变更
- DDL 和 DML 使用相同的 LSN 序列，可按 LSN 排序合并
- 需要按 LSN 升序处理，确保 DDL 在对应的 DML 之前执行

## 二、核心实现步骤

### 步骤 1：添加 DDL 相关 SQL 常量和字段

```java
// DDL 相关 SQL
private static final String GET_DDL_CHANGES = 
    "SELECT source_object_id, object_id, ddl_command, ddl_lsn, ddl_time " +
    "FROM cdc.ddl_history " +
    "WHERE ddl_lsn > ? AND ddl_lsn <= ? " +
    "ORDER BY ddl_lsn ASC";

private static final String DDL_LSN_POSITION = "ddl_position";

// 字段
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.TimeUnit;

private Lsn lastDdlLsn;  // 最后处理的 DDL LSN

// 表名缓存：使用 Caffeine 本地缓存，10分钟过期
private final Cache<Integer, String> objectIdToTableNameCache = Caffeine.newBuilder()
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .maximumSize(1000)
    .build();
```

### 步骤 2：修改 readLastLsn() 方法

```java
private void readLastLsn() {
    // 读取 DML LSN（现有逻辑）
    if (!snapshot.containsKey(LSN_POSITION)) {
        lastLsn = queryAndMap(GET_MAX_LSN, rs -> new Lsn(rs.getBytes(1)));
        if (null != lastLsn && lastLsn.isAvailable()) {
            snapshot.put(LSN_POSITION, lastLsn.toString());
            super.forceFlushEvent();
            return;
        }
        throw new SqlServerException("No maximum LSN recorded in the database");
    }
    lastLsn = Lsn.valueOf(snapshot.get(LSN_POSITION));
    
    // 读取 DDL LSN（新增逻辑）
    if (!snapshot.containsKey(DDL_LSN_POSITION)) {
        lastDdlLsn = lastLsn;
        snapshot.put(DDL_LSN_POSITION, lastDdlLsn.toString());
    } else {
        lastDdlLsn = Lsn.valueOf(snapshot.get(DDL_LSN_POSITION));
    }
}
```

### 步骤 3：创建 DDLEvent 模型类

```java
package org.dbsyncer.connector.sqlserver.model;

import org.dbsyncer.connector.sqlserver.cdc.Lsn;
import java.util.Date;

public class DDLEvent {
    private String tableName;
    private String ddlCommand;
    private Lsn ddlLsn;
    private Date ddlTime;
    
    public DDLEvent(String tableName, String ddlCommand, Lsn ddlLsn, Date ddlTime) {
        this.tableName = tableName;
        this.ddlCommand = ddlCommand;
        this.ddlLsn = ddlLsn;
        this.ddlTime = ddlTime;
    }
    
    // getter/setter 方法
}
```

### 步骤 4：创建 UnifiedChangeEvent 统一事件模型

```java
package org.dbsyncer.connector.sqlserver.model;

import org.dbsyncer.connector.sqlserver.cdc.Lsn;

public class UnifiedChangeEvent {
    private Lsn lsn;
    private String eventType;  // "DDL" 或 "DML"
    private String tableName;
    
    // DDL 相关
    private String ddlCommand;
    
    // DML 相关
    private CDCEvent cdcEvent;
    
    // getter/setter 方法
}
```

### 步骤 5：修改 CDCEvent 类，添加 LSN 字段

```java
package org.dbsyncer.connector.sqlserver.model;

import org.dbsyncer.connector.sqlserver.cdc.Lsn;
import java.util.List;

public final class CDCEvent {
    private String tableName;
    private int code;
    private List<Object> row;
    private Lsn lsn;  // 新增：包含 LSN 信息，用于事件排序

    public CDCEvent(String tableName, int code, List<Object> row, Lsn lsn) {
        this.tableName = tableName;
        this.code = code;
        this.row = row;
        this.lsn = lsn;
    }
    
    // 保持向后兼容的构造函数
    public CDCEvent(String tableName, int code, List<Object> row) {
        this(tableName, code, row, null);
    }

    public String getTableName() { return tableName; }
    public int getCode() { return code; }
    public List<Object> getRow() { return row; }
    public Lsn getLsn() { return lsn; }
}
```

### 步骤 6：修改 pull() 方法，同时查询 DDL 和 DML

```java
private void pull(Lsn stopLsn) {
    Lsn startLsn = queryAndMap(GET_INCREMENT_LSN, 
        statement -> statement.setBytes(1, lastLsn.getBinary()), 
        rs -> Lsn.valueOf(rs.getBytes(1)));
    
    // 1. 查询 DDL 变更
    List<DDLEvent> ddlEvents = pullDDLChanges(startLsn, stopLsn);
    
    // 2. 查询 DML 变更（修改：提取 LSN）
    List<CDCEvent> dmlEvents = new ArrayList<>();
    changeTables.forEach(changeTable -> {
        final String query = GET_ALL_CHANGES_FOR_TABLE
            .replace(STATEMENTS_PLACEHOLDER, changeTable.getCaptureInstance());
        List<CDCEvent> list = queryAndMapList(query, statement -> {
            statement.setBytes(1, startLsn.getBinary());
            statement.setBytes(2, stopLsn.getBinary());
        }, rs -> {
            int columnCount = rs.getMetaData().getColumnCount();
            List<Object> row = null;
            List<CDCEvent> data = new ArrayList<>();
            
            while (rs.next()) {
                final int operation = rs.getInt(3);  // __$operation 是第 3 列
                if (TableOperationEnum.isUpdateBefore(operation)) {
                    continue;
                }
                
                // 提取 LSN（第 1 列是 __$start_lsn）
                Lsn eventLsn = new Lsn(rs.getBytes(1));
                
                row = new ArrayList<>(columnCount - OFFSET_COLUMNS);
                for (int i = OFFSET_COLUMNS + 1; i <= columnCount; i++) {
                    row.add(rs.getObject(i));
                }
                
                // 创建 CDCEvent，包含 LSN 信息
                data.add(new CDCEvent(changeTable.getTableName(), operation, row, eventLsn));
            }
            return data;
        });
        
        if (!CollectionUtils.isEmpty(list)) {
            dmlEvents.addAll(list);
        }
    });
    
    // 3. 合并并按 LSN 排序
    List<UnifiedChangeEvent> unifiedEvents = mergeAndSortEvents(ddlEvents, dmlEvents);
    
    // 4. 按顺序解析和发送
    parseUnifiedEvents(unifiedEvents, stopLsn);
}
```

### 步骤 7：实现 pullDDLChanges() 方法

```java
private List<DDLEvent> pullDDLChanges(Lsn startLsn, Lsn stopLsn) {
    Lsn ddlStartLsn = lastDdlLsn;
    if (ddlStartLsn.compareTo(startLsn) < 0) {
        ddlStartLsn = queryAndMap(GET_INCREMENT_LSN, 
            statement -> statement.setBytes(1, lastDdlLsn.getBinary()), 
            rs -> Lsn.valueOf(rs.getBytes(1)));
    }
    
    return queryAndMapList(GET_DDL_CHANGES, statement -> {
        statement.setBytes(1, ddlStartLsn.getBinary());
        statement.setBytes(2, stopLsn.getBinary());
    }, rs -> {
        List<DDLEvent> events = new ArrayList<>();
        while (rs.next()) {
            int sourceObjectId = rs.getInt(1);
            String ddlCommand = rs.getString(3);
            Lsn ddlLsn = new Lsn(rs.getBytes(4));
            Date ddlTime = rs.getTimestamp(5);
            
            // 根据 object_id 查找完整表名（包含 schema）
            String fullTableName = getTableNameByObjectId(sourceObjectId);
            if (fullTableName == null) {
                logger.warn("无法找到 object_id={} 对应的表，可能已被删除，跳过该 DDL 事件", sourceObjectId);
                continue;
            }
            
            // 过滤 DDL 类型：只处理 ALTER TABLE
            if (!isTableAlterDDL(ddlCommand)) {
                continue;
            }
            
            // 解析 schema 和表名
            String[] parts = parseSchemaAndTableName(fullTableName);
            String tableSchema = parts[0];
            String tableName = parts[1];
            
            // 检查 schema 和表名是否匹配
            if (isFilterTable(tableSchema, tableName)) {
                events.add(new DDLEvent(tableName, ddlCommand, ddlLsn, ddlTime));
            }
        }
        return events;
    });
}

private String getTableNameByObjectId(int objectId) {
    return objectIdToTableNameCache.get(objectId, id -> {
        return queryAndMap(
            "SELECT s.name + '.' + t.name FROM sys.tables t " +
            "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id " +
            "WHERE t.object_id = ?",
            statement -> statement.setInt(1, id),
            rs -> rs.next() ? rs.getString(1) : null
        );
    });
}

private String[] parseSchemaAndTableName(String fullTableName) {
    if (fullTableName == null) {
        return new String[]{null, null};
    }
    int dotIndex = fullTableName.indexOf('.');
    if (dotIndex > 0) {
        String schema = fullTableName.substring(0, dotIndex);
        String tableName = fullTableName.substring(dotIndex + 1);
        return new String[]{schema, tableName};
    }
    return new String[]{schema, fullTableName};
}

private boolean isTableAlterDDL(String ddlCommand) {
    if (ddlCommand == null) {
        return false;
    }
    return ddlCommand.toUpperCase().trim().startsWith("ALTER TABLE");
}

private boolean isFilterTable(String tableSchema, String tableName) {
    if (schema != null && !schema.equalsIgnoreCase(tableSchema)) {
        return false;
    }
    return filterTable.contains(tableName);
}
```

### 步骤 8：实现 mergeAndSortEvents() 方法

```java
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
    changeTables.forEach(changeTable -> {
        dmlEvents.stream()
            .filter(e -> e.getTableName().equals(changeTable.getTableName()))
            .forEach(dmlEvent -> {
                UnifiedChangeEvent event = new UnifiedChangeEvent();
                event.setLsn(dmlEvent.getLsn());
                event.setEventType("DML");
                event.setTableName(dmlEvent.getTableName());
                event.setCdcEvent(dmlEvent);
                unifiedEvents.add(event);
            });
    });
    
    // 按 LSN 排序
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

### 步骤 9：实现 parseUnifiedEvents() 方法

```java
private void parseUnifiedEvents(List<UnifiedChangeEvent> events, Lsn stopLsn) {
    int size = events.size();
    for (int i = 0; i < size; i++) {
        boolean isEnd = i == size - 1;
        UnifiedChangeEvent unifiedEvent = events.get(i);
        
        if ("DDL".equals(unifiedEvent.getEventType())) {
            trySendDDLEvent(
                unifiedEvent.getTableName(),
                unifiedEvent.getDdlCommand(),
                unifiedEvent.getLsn(),
                isEnd ? stopLsn : null
            );
            // 更新 DDL LSN
            if (isEnd) {
                lastDdlLsn = stopLsn;
                snapshot.put(DDL_LSN_POSITION, lastDdlLsn.toString());
            }
        } else {
            // 发送 DML 事件（现有逻辑）
            CDCEvent cdcEvent = unifiedEvent.getCdcEvent();
            if (TableOperationEnum.isUpdateAfter(cdcEvent.getCode())) {
                trySendEvent(new RowChangedEvent(
                    cdcEvent.getTableName(), 
                    ConnectorConstant.OPERTION_UPDATE, 
                    cdcEvent.getRow(), 
                    null, 
                    (isEnd ? stopLsn : null)
                ));
            } else if (TableOperationEnum.isInsert(cdcEvent.getCode())) {
                trySendEvent(new RowChangedEvent(
                    cdcEvent.getTableName(), 
                    ConnectorConstant.OPERTION_INSERT, 
                    cdcEvent.getRow(), 
                    null, 
                    (isEnd ? stopLsn : null)
                ));
            } else if (TableOperationEnum.isDelete(cdcEvent.getCode())) {
                trySendEvent(new RowChangedEvent(
                    cdcEvent.getTableName(), 
                    ConnectorConstant.OPERTION_DELETE, 
                    cdcEvent.getRow(), 
                    null, 
                    (isEnd ? stopLsn : null)
                ));
            }
        }
    }
}
```

### 步骤 10：实现 trySendDDLEvent() 方法

```java
private void trySendDDLEvent(String tableName, String ddlCommand, Lsn ddlLsn, Lsn position) {
    String actualTableName = extractTableNameFromDDL(ddlCommand, tableName);
    
    if (actualTableName != null && filterTable.contains(actualTableName)) {
        DDLChangedEvent ddlEvent = new DDLChangedEvent(
            actualTableName,
            ConnectorConstant.OPERTION_ALTER,
            ddlCommand,
            null,
            position
        );
        
        while (connected) {
            try {
                sendChangedEvent(ddlEvent);
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
}

private String extractTableNameFromDDL(String ddlCommand, String defaultTableName) {
    try {
        Statement statement = CCJSqlParserUtil.parse(ddlCommand);
        if (statement instanceof Alter) {
            Alter alter = (Alter) statement;
            String tableName = alter.getTable().getName();
            return StringUtil.replace(tableName, StringUtil.BACK_QUOTE, StringUtil.EMPTY);
        }
    } catch (JSQLParserException e) {
        logger.warn("无法解析 DDL 语句: {}", ddlCommand);
    }
    return defaultTableName;
}
```

## 三、关键注意事项

### 1. Schema 处理
- 从 `cdc.ddl_history` 查询时，需要同时检查 schema 和表名
- 使用 `parseSchemaAndTableName()` 解析完整表名
- 使用 `isFilterTable(tableSchema, tableName)` 验证

### 2. DDL 类型过滤
- 只处理 `ALTER TABLE` 相关的 DDL
- 过滤 `CREATE INDEX`、`ALTER INDEX`、`DROP INDEX` 等其他 DDL

### 3. 表删除场景
- 如果 `getTableNameByObjectId()` 返回 `null`，记录警告日志并跳过

### 4. 时序保证
- 必须按 LSN 升序排序，确保 DDL 在对应的 DML 之前处理
- 排序使用 `Lsn.compareTo()` 方法

### 5. 性能优化
- 使用 Caffeine 缓存表名映射，10分钟过期
- 提前过滤非 ALTER TABLE 的 DDL
- 批量查询 DDL 变更
