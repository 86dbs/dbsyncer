# 移除定时任务，优化 Snapshot 持久化机制

## 一、问题分析

### 1.1 定时任务时间窗口风险
- 数据同步完成后，`refreshEvent` 只更新内存 snapshot，不立即持久化
- 依赖 3 秒定时任务持久化，存在时间窗口风险
- 系统崩溃时，可能从错误位置恢复，导致数据丢失

### 1.2 TABLE_MAP 事件缺失风险
**场景**：
- MySQL binlog 事件顺序：`TABLE_MAP → WRITE_ROWS(1-30) → WRITE_ROWS(31-100) → XID`
- 前 30 条批量同步完成，快照点持久化在第 30 条位置
- 系统崩溃重启，从第 30 条位置继续读取
- **问题**：第 31-100 条 WRITE_ROWS 事件需要 TABLE_MAP 来解析，但 TABLE_MAP 在第 1 条之前已被跳过

**影响**：
- `BinaryLogRemoteClient` 反序列化失败，跳过事件 → **数据丢失**
- 或 `tables.get(tableId)` 返回 null，抛出 `NullPointerException` → **后续事件无法处理**

**根本原因**：
- `tables` Map（`TableMapEventData` 缓存）是内存缓存，**不持久化**
- 重启后 `tables` 为空，从中间位置读取会跳过 TABLE_MAP 事件，导致缓存无法重建

### 1.3 IO 频繁问题（实施后需关注）
**核心问题**：非任务事件数量远大于任务事件
- MySQL binlog 是全局的，包含整个实例的所有事务事件
- 当前同步任务通常只监控少数几个表
- 所有 XID 事件（包括非任务事件）都会更新快照点并触发持久化
- **影响**：任务事件占比越小，IO 浪费越严重（如 10 任务事件/秒 + 990 非任务事件/秒 = 1000 次/秒 IO）

## 二、解决方案

### 2.1 核心思路
1. **快照点使用事务提交位置（XID 事件）**：确保 TABLE_MAP 事件已被处理
2. **数据同步完成后更新快照点**：移除 DML 事件中的 `refresh(header)` 调用
3. **异步批量持久化**：解决非任务事件导致的 IO 频繁问题

### 2.2 设计原则
- DML 事件：快照点使用 XID 事件位置（事务提交位置）
- DDL 事件：保持当前机制（QUERY 事件位置，因为 DDL 没有 XID 事件）
- 持久化策略：采用异步批量持久化，每 3 秒写入最新快照（平衡 IO 频率和数据安全性）

## 三、代码修改

### 修改1：MySQLListener - DML 事件移除 refresh 调用

**文件**: `dbsyncer-connector/dbsyncer-connector-mysql/src/main/java/org/dbsyncer/connector/mysql/cdc/MySQLListener.java`

**修改内容**：移除 DML 事件（WRITE_ROWS/UPDATE_ROWS/DELETE_ROWS）中的 `refresh(header)` 调用

```java
// 修改前：每个 DML 事件都调用 refresh(header)
if (notUniqueCodeEvent && EventType.isUpdate(header.getEventType())) {
    refresh(header);  // ❌ 移除这行
    UpdateRowsEventData data = event.getData();
    // ... 处理逻辑
}

if (notUniqueCodeEvent && EventType.isWrite(header.getEventType())) {
    refresh(header);  // ❌ 移除这行
    WriteRowsEventData data = event.getData();
    // ... 处理逻辑
}

if (notUniqueCodeEvent && EventType.isDelete(header.getEventType())) {
    refresh(header);  // ❌ 移除这行
    DeleteRowsEventData data = event.getData();
    // ... 处理逻辑
}

// 修改后：只在 XID 事件时调用 refresh(header)
if (header.getEventType() == EventType.XID) {
    refresh(header);  // ✅ 保留，这是事务提交位置
    return;
}
```

**说明**：
- XID 事件表示事务提交，此时所有 DML 事件已处理完成
- 在 XID 事件时更新快照点，确保 TABLE_MAP 事件已被处理
- DDL 事件（QUERY）保持当前逻辑，因为 DDL 没有 XID 事件

### 修改2：AbstractListener - 实现异步批量持久化

**文件**: `dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/listener/AbstractListener.java`

**修改内容**：添加异步批量持久化机制，每 3 秒写入最新快照

```java
// 添加字段
private volatile Map<String, String> pendingSnapshot = null;
private ScheduledExecutorService flushExecutor;

// 初始化方法（在构造函数或 @PostConstruct 中调用）
private void initFlushExecutor() {
    flushExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "snapshot-flush-" + metaId);
        t.setDaemon(true);
        return t;
    });
    // 每 3 秒检查并写入最新快照
    flushExecutor.scheduleWithFixedDelay(() -> {
        Map<String, String> snapshot = pendingSnapshot;
        if (snapshot != null && hasSnapshotChanged(snapshot, lastFlushedSnapshot)) {
            try {
                logger.info("snapshot changed, flushing: {}", snapshot);
                watcher.flushEvent(snapshot);
                lastFlushedSnapshot = new HashMap<>(snapshot);
                pendingSnapshot = null;
            } catch (Exception e) {
                logger.error("异步持久化失败", e);
            }
        }
    }, 3, 3, TimeUnit.SECONDS);
}

// 修改 flushEvent 方法
@Override
public void flushEvent() throws Exception {
    if (CollectionUtils.isEmpty(snapshot)) {
        return;
    }
    
    if (hasSnapshotChanged(snapshot, lastFlushedSnapshot)) {
        // 只更新待持久化的快照点，不立即写入
        // 定时任务会每3秒检查并写入最新快照
        pendingSnapshot = new HashMap<>(snapshot);
    }
}

// 添加 @PreDestroy 方法，确保系统关闭时立即持久化
@PreDestroy
public void destroy() {
    if (flushExecutor != null) {
        flushExecutor.shutdown();
        try {
            // 等待正在执行的任务完成
            if (!flushExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                flushExecutor.shutdownNow();
            }
            // 系统关闭时立即持久化最新快照
            Map<String, String> snapshot = pendingSnapshot;
            if (snapshot != null && hasSnapshotChanged(snapshot, lastFlushedSnapshot)) {
                watcher.flushEvent(snapshot);
            }
        } catch (InterruptedException e) {
            flushExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
```

**说明**：
- 数据同步完成后，`flushEvent()` 只更新 `pendingSnapshot`，不立即写入数据库
- 定时任务每 3 秒检查并写入最新快照，大幅降低 IO 频率
- 系统关闭时立即持久化，确保不丢失最新快照点

### 修改3：SCAN 事件补充 refreshOffset

**文件**: `dbsyncer-parser/src/main/java/org/dbsyncer/parser/flush/impl/GeneralBufferActuator.java`

**修改内容**：在 SCAN 事件处理中添加 `refreshOffset` 调用

```java
case SCAN:
    pickers.forEach(picker -> {
        distributeTableGroup(response, mapping, picker, picker.getSourceFields(), false);
    });
    bufferActuatorRouter.refreshOffset(response.getChangedOffset());  // ✅ 补充
    break;
```

### 修改4：移除定时任务

**文件**: `dbsyncer-manager/src/main/java/org/dbsyncer/manager/impl/IncrementPuller.java`

**修改内容**：移除 3 秒定时任务

```java
@PostConstruct
private void init() {
    // ❌ 移除定时任务：数据同步完成后更新快照点，异步批量持久化，不再需要定时任务
    // scheduledTaskService.start(3000, this);  // 注释掉或删除
}
```

## 四、事件类型影响

| 事件类型 | 快照点更新时机 | 持久化策略 | 备注 |
|---------|--------------|-----------|------|
| DML (ROW) | XID 事件时更新 | 异步批量持久化（每3秒） | 事务提交位置，确保 TABLE_MAP 已处理 |
| DDL (QUERY) | QUERY 事件时更新 | 异步批量持久化（每3秒） | DDL 自动提交，无 XID 事件，保持当前机制 |
| SCAN | 需补充 refreshOffset | 异步批量持久化（每3秒） | 需添加 refreshOffset 调用 |

## 五、实施计划

### 5.1 修改清单
1. ✅ `MySQLListener.onEvent()` - DML 事件移除 `refresh(header)` 调用（3处：UPDATE/INSERT/DELETE）
2. ✅ `AbstractListener` - 实现异步批量持久化机制
3. ✅ `GeneralBufferActuator.pull()` - SCAN 事件补充 refreshOffset
4. ✅ `IncrementPuller.init()` - 移除定时任务

### 5.2 测试验证
- [ ] 数据同步完成后快照点正确更新（XID 事件位置）
- [ ] 系统崩溃后从正确位置恢复（XID 事件位置，TABLE_MAP 已处理）
- [ ] IO 频率显著降低（从每秒 N 次降低到每 3 秒 1 次）
- [ ] 系统关闭时立即持久化最新快照点
- [ ] 性能无明显下降

### 5.3 回滚方案
1. 恢复 DML 事件中的 `refresh(header)` 调用
2. 移除 `AbstractListener` 中的异步批量持久化机制
3. 恢复 `IncrementPuller` 中的定时任务
4. 重新部署

## 六、优势与注意事项

### 6.1 优势
1. **数据一致性**：快照点使用 XID 事件位置，确保 TABLE_MAP 已处理
2. **风险消除**：消除定时任务时间窗口风险
3. **IO 优化**：异步批量持久化，大幅降低 IO 频率（从每秒 N 次降低到每 3 秒 1 次）
4. **逻辑清晰**：移除定时任务，避免逻辑冲突

### 6.2 注意事项
- ⚠️ **延迟 3 秒持久化**：最多延迟 3 秒持久化，数据丢失风险窗口较小（比原定时任务更安全，因为快照点位置更准确）
- ⚠️ **系统关闭时必须立即持久化**：已实现 `@PreDestroy` 钩子，确保不丢失最新快照点
- ⚠️ **非任务事件也会更新快照点**：这是正常且必要的设计，因为 binlog 是顺序读取的，位置必须前进
- ⚠️ **DDL 事件保持当前机制**：DDL 没有 XID 事件，使用 QUERY 事件位置是合理的

## 七、非当前任务事件的处理说明

### 7.1 设计说明
- MySQL binlog 是全局的，包含整个实例的所有数据库/表的事件
- 当前任务只监控部分表（通过 `filterTable` 过滤）
- 非监控表的事件也会被读取，但不会发送同步事件

### 7.2 快照点更新机制
- **XID 事件**：所有 XID 事件都会更新快照点（无论事务中是否有当前任务的事件）
- **必要性**：binlog 是顺序读取的，位置必须前进，快照点记录的是 binlog 读取位置，不是数据同步位置
- **影响**：重启后通过 `isFilterTable` 过滤，只处理属于当前任务的事件，不影响数据一致性

### 7.3 新方案的影响
- ✅ 非任务事件仍然会更新快照点（通过 XID 事件）
- ✅ 更新频率降低（从每个 DML 事件改为每个 XID 事件）
- ✅ 更符合事务边界，快照点位置更安全
- ✅ 异步批量持久化，IO 频率与任务事件数量无关
