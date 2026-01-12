# 快照机制简化方案

## 一、当前实现的问题

### 1.1 复杂度高
- 增加了 `taskSnapshot` 变量（volatile Map），需要跨线程维护
- 增加了 `pendingTaskDataCount` 计数机制，需要确保增加和减少平衡
- 需要区分任务事件和非任务事件的快照位置

### 1.2 潜在问题
- 快照位置难以维护：`taskSnapshot` 在不同线程间共享，可能有并发问题
- 计数不平衡：如果事件发送成功但处理失败，计数可能无法正确减少

## 二、解决方案

### 2.1 核心思路

**关键发现**：ROW 事件携带的是**上一个事务的 XID**位置！

**binlog事件顺序**（单线程，固定顺序）：
```
TABLE_MAP > WRITE_ROWS > UPDATE_ROWS > DELETE_ROWS > XID
```

**时序分析**：
1. **XID1 到达** → `refresh(header)` → `snapshot` = XID1位置
2. **ROW2 发送** → 携带当前的 `snapshot`（XID1位置）→ 进入队列
3. **XID2 到达** → `refresh(header)` → `snapshot` = XID2位置
4. **ROW3 发送** → 携带当前的 `snapshot`（XID2位置）→ 进入队列
5. **队列处理**：
   - ROW2处理完成 → `refreshOffset()` → 使用ROW2携带的XID1位置 ✅
   - ROW3处理完成 → `refreshOffset()` → 使用ROW3携带的XID2位置 ✅

**解决方案**：
1. **ROW事件携带上一个事务的XID**：ROW事件发送时，携带当前的 `snapshot`（上一个事务的XID位置）
2. **refreshOffset时直接使用**：直接使用ROW事件携带的XID位置更新 `snapshot` 和 `pendingSnapshot`
3. **任务事件期间阻塞非任务事件**：使用简单的布尔标记 + 队列状态判断，任务队列为空则清除 pending 状态，如果有任务在处理，非任务事件的XID不更新 `pendingSnapshot`，防止快照位置相互覆盖

### 2.2 方案优势

**优点**：
- **数据安全**：每个ROW事件都携带了正确的XID位置（上一个事务的XID），避免数据丢失
- **保持实时性**：ROW 事件立即发送，不延迟
- **逻辑简单**：不需要事务映射表，不需要taskSnapshot，ROW事件直接携带XID位置
- **防止覆盖**：任务事件期间阻塞非任务事件刷新快照，防止快照位置相互覆盖
- **代码简洁**：移除了taskSnapshot和计数机制，使用简单的布尔标记 + 队列状态判断，更简单直观

**缺点**：
- 非任务事件的快照位置可能延迟更新（不影响数据同步，只影响监控）

## 三、实现细节

### 3.1 XID 事件处理

**逻辑**：
- **有任务在处理**：**不刷新快照**（任务事件携带的XID快照不应该能够修改），只更新 client 的 binlog 位置，保持位置同步
- **没有任务在处理**：正常更新 `snapshot` 和 `pendingSnapshot`

**关键设计**：
- **任务事件的 XID 不刷新快照**：防止任务事件的 XID 覆盖 `snapshot`，导致后续 ROW 事件携带错误的快照位置
- **任务事件的快照位置由 `refreshEvent()` 直接设置**：不依赖 `onEvent()` 中的 `refresh()`，避免新旧任务相互覆盖

**代码位置**：`MySQLListener.onEvent()` - XID事件处理

```java
if (header.getEventType() == EventType.XID) {
    if (!hasPendingTaskData()) {
        // 没有任务数据在处理：非任务事件的 XID，正常更新 snapshot 和 pendingSnapshot
        refresh(header);  // 更新 snapshot
        try {
            flushEvent();  // 更新 pendingSnapshot
        } catch (Exception e) {
            logger.warn("非任务事件 XID 更新 pendingSnapshot 失败", e);
        }
    } else {
        // 有任务数据在处理：任务事件的 XID，不刷新快照
        // 原因：任务事件携带的XID快照不应该能够修改，防止新旧任务相互覆盖
        // 任务事件的快照位置由 refreshEvent() 直接设置到 pendingSnapshot，不依赖 onEvent() 中的 refresh()
        // 只更新 client 的 binlog 位置，保持位置同步，但不更新 snapshot
        EventHeaderV4 eventHeaderV4 = (EventHeaderV4) header;
        if (0 < eventHeaderV4.getNextPosition()) {
            client.setBinlogPosition(eventHeaderV4.getNextPosition());
        }
    }
    return;
}
```

### 3.2 ROW 事件发送

**逻辑**：ROW事件发送时，携带当前的 `snapshot`（上一个事务的XID位置）到 `ChangedOffset` 中

**代码位置**：`MySQLListener.onEvent()` - ROW事件处理（WRITE_ROWS、UPDATE_ROWS、DELETE_ROWS）

**修改点**：将 `client.getBinlogFilename()` 和 `client.getBinlogPosition()` 改为使用 `snapshot.get(BINLOG_FILENAME)` 和 `snapshot.get(BINLOG_POSITION)`

```java
// ROW 事件发送时，携带当前的 snapshot（上一个事务的 XID 位置）
RowChangedEvent rowEvent = new RowChangedEvent(
    getTableName(tableId), 
    ConnectorConstant.OPERTION_INSERT, 
    after, 
    snapshot.get(BINLOG_FILENAME),  // 上一个事务的 XID 文件名
    snapshot.get(BINLOG_POSITION) != null ? Long.parseLong(snapshot.get(BINLOG_POSITION)) : null,  // 上一个事务的 XID 位置
    columnNames
);
trySendEvent(rowEvent);
```

### 3.3 refreshEvent 实现

**逻辑**：直接使用任务携带的 XID 位置更新 `snapshot` 和 `pendingSnapshot`，**不依赖共享的 snapshot**，防止新旧任务相互覆盖

**代码位置**：`MySQLListener.refreshEvent()`

**关键设计**：
- **任务事件携带的XID快照不应该能够修改**：直接使用任务携带的位置，不依赖共享的 `snapshot`
- **直接更新 pendingSnapshot**：使用任务携带的位置创建新的 `pendingSnapshot`，避免并发任务覆盖
- **同时更新 snapshot**：用于后续 ROW 事件携带，保持位置同步

**并发安全**：
- **旧任务完成**：直接使用任务携带的位置更新 `pendingSnapshot`，不会被新任务覆盖
- **新任务完成**：直接使用任务携带的位置更新 `pendingSnapshot`，不会被旧任务覆盖

```java
@Override
public void refreshEvent(ChangedOffset offset) {
    // ROW 事件已经携带了上一个事务的 XID 位置
    // 关键：任务事件携带的XID快照不应该能够修改，直接使用任务携带的位置更新 pendingSnapshot
    // 不更新共享的 snapshot，防止新旧任务相互覆盖
    if (offset != null && offset.getPosition() != null && offset.getNextFileName() != null) {
        String newFileName = offset.getNextFileName();
        Long newPosition = (Long) offset.getPosition();
        
        // 直接使用任务携带的位置更新 snapshot（用于后续 ROW 事件携带）
        // 注意：这里仍然需要更新 snapshot，因为后续的 ROW 事件需要携带这个位置
        refreshSnapshot(newFileName, newPosition);
        
        // 同时更新 pendingSnapshot，使用任务携带的位置（不依赖共享的 snapshot）
        // 这样可以避免并发任务覆盖问题
        Map<String, String> taskSnapshot = new HashMap<>();
        taskSnapshot.put(BINLOG_FILENAME, newFileName);
        taskSnapshot.put(BINLOG_POSITION, String.valueOf(newPosition));
        setPendingSnapshot(taskSnapshot);
    }
}
```

### 3.4 flushEvent 简化

**逻辑**：移除 `taskSnapshot` 相关逻辑，直接使用当前的 `snapshot` 更新 `pendingSnapshot`

**代码位置**：`AbstractListener.flushEvent()`

```java
public void flushEvent() {
    if (CollectionUtils.isEmpty(snapshot)) {
        return;
    }
    // 直接使用当前的 snapshot 更新 pendingSnapshot
    pendingSnapshot = new HashMap<>(snapshot);
}
```

### 3.5 使用队列状态判断

**逻辑**：使用简单的布尔标记 + 队列状态判断，任务队列为空则清除 pending 状态

**代码位置**：`ParserConsumer`、`BufferActuatorRouter`

**实现内容**：
- **使用** `hasPendingTask` 变量（`volatile boolean`），在 `ParserConsumer` 中标记是否有任务进入队列
- **自动设置**：`ParserConsumer.changeEvent()` 自动设置 `hasPendingTask = true`（所有到达 changeEvent 的事件都是任务事件）
- **自动清除**：`ParserConsumer.checkAndClearPendingTask()` 方法（任务事件完成时调用，如果队列为空则清除标记）
- **查询方法**：`hasPendingTaskData()` 方法在 `AbstractListener` 中通过 `watcher.hasPendingTask()` 查询
- **移除** `taskSnapshot` 变量
- **移除** `getTaskSnapshot()` 方法
- **移除** 计数机制（`pendingTaskDataCount`、`incrementPendingTaskData()`、`decrementPendingTaskData()`）
- `flushEvent()` 直接使用 `snapshot`，不再使用 `getTaskSnapshot()`
- `BufferActuatorRouter.refreshOffset()` 调用 `checkAndClearPendingTask()`（检查队列是否为空，如果为空则清除标记）

## 四、实施步骤

### 4.1 核心修改

1. **移除 `taskSnapshot` 变量**：从 `AbstractListener` 中移除 `taskSnapshot` 及其相关逻辑（`getTaskSnapshot()` 方法）
2. **移除计数机制**：从 `AbstractListener` 中移除 `pendingTaskDataCount`、`incrementPendingTaskData()`、`decrementPendingTaskData()`，改用简单的布尔标记 + 队列状态判断
3. **修改 XID 事件处理**：在 `MySQLListener.onEvent()` 中，有任务在处理时，XID事件只更新 `snapshot`，不更新 `pendingSnapshot`
4. **修改 ROW 事件发送**：在 `MySQLListener.onEvent()` 中，ROW事件发送时使用 `snapshot` 而不是 `client.getBinlogFilename()` 和 `client.getBinlogPosition()`
5. **实现 refreshEvent**：在 `MySQLListener.refreshEvent()` 中，使用 `ChangedOffset` 中的位置更新 `snapshot`
6. **简化 flushEvent**：在 `AbstractListener.flushEvent()` 中，直接使用 `snapshot` 更新 `pendingSnapshot`

### 4.2 工作流程

**任务事件流程**：
1. **XID1 到达**（任务事件的XID）→ 检查 `hasPendingTaskData()` → 没有任务在处理（ROW 事件还未发送），正常更新 `snapshot` 和 `pendingSnapshot`
2. **ROW2 发送** → 携带当前的 `snapshot`（XID1位置）→ `changeEvent()` → 自动设置 `hasPendingTask = true` → 进入队列
3. **XID2 到达**（下一个事务的XID）→ 检查 `hasPendingTaskData()` → 有任务在处理，**不刷新快照**（只更新 client 的 binlog 位置）
4. **数据同步完成** → `refreshOffset()` → `refreshEvent(offset)` → 直接使用ROW2携带的XID1位置更新 `snapshot` 和 `pendingSnapshot`（不依赖共享的 snapshot）→ `checkAndClearPendingTask()` → 如果队列为空，清除 `hasPendingTask`

**非任务事件流程**：
1. **XID 到达**（非任务事件的XID）→ `refresh(header)` → 更新 `snapshot`（XID 位置）→ 检查 `hasPendingTaskData()` → 没有任务在处理，`flushEvent()` → 更新 `pendingSnapshot`

### 4.3 注意事项

- **关键**：ROW 事件携带的是**上一个事务的 XID**位置，而不是当前事务的 XID 位置。由于事件顺序固定，ROW 事件发送时，上一个事务的 XID 已经到达并更新了 `snapshot`
- **第一个事务的处理**：第一个事务的ROW事件可能没有上一个事务的XID，需要使用初始位置或null
- **非任务事件快照延迟**：如果有大量任务事件在处理，非任务事件的快照位置可能长时间不更新
  - **影响**：如果系统崩溃，非任务事件的快照位置可能落后
  - **缓解**：非任务事件的快照位置不影响数据同步，只是用于监控
- **使用队列状态判断**：任务队列为空则清除 pending 状态，比计数机制更简单直观
- **防止快照覆盖**：任务事件期间阻塞非任务事件刷新快照，防止快照位置相互覆盖导致回退
- **任务事件的XID不刷新快照**：在 `onEvent()` 中，任务事件的 XID 不刷新快照，防止任务事件的 XID 覆盖 `snapshot`
- **直接使用任务携带的位置**：`refreshEvent()` 直接使用任务携带的位置更新 `pendingSnapshot`，不依赖共享的 `snapshot`，避免并发任务覆盖
- **不需要修改 `ChangedOffset`**：直接使用现有的 `position` 和 `nextFileName` 字段

## 五、验证要点

1. **XID事件处理**：验证任务事件的XID只更新 `snapshot`，非任务事件的XID更新 `snapshot` 和 `pendingSnapshot`
2. **ROW事件位置**：验证ROW事件发送时是否携带了正确的XID位置（上一个事务的XID）
3. **refreshEvent**：验证 `refreshEvent()` 是否直接使用任务携带的位置更新 `pendingSnapshot`，不依赖共享的 `snapshot`
4. **flushEvent**：验证 `flushEvent()` 是否正确更新了 `pendingSnapshot`
5. **队列状态判断**：验证任务队列为空时，pending 状态被正确清除
6. **快照覆盖防护**：验证任务事件期间，非任务事件的XID不会更新 `pendingSnapshot`
