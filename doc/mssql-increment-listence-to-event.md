# SQL Server 增量数据同步实现机制

## 一、概述

本文档详细梳理了 DBSyncer 中 SQL Server 增量数据同步的实现机制，包括如何监听数据变更、如何处理变更事件以及如何发送数据事件到上层系统。

### 1.1 技术架构

SQL Server 增量同步基于 **CDC (Change Data Capture)** 机制实现，采用**双线程 + 轮询**的架构设计：

- **LsnPuller**：全局单例，负责定期轮询获取最新的 LSN（Log Sequence Number）
- **SqlServerListener.Worker**：每个监听器的工作线程，负责处理变更数据并发送事件

### 1.2 核心组件

| 组件                  | 职责                | 位置                                                       |
| ------------------- | ----------------- | -------------------------------------------------------- |
| `SqlServerListener` | 监听器主类，管理 CDC 生命周期 | `org.dbsyncer.connector.sqlserver.cdc.SqlServerListener` |
| `LsnPuller`         | 全局 LSN 轮询器，单例模式   | `org.dbsyncer.connector.sqlserver.cdc.LsnPuller`         |
| `Worker`            | 工作线程，处理变更数据       | `SqlServerListener.Worker`                               |
| `Lsn`               | LSN 值对象，表示日志序列号   | `org.dbsyncer.connector.sqlserver.cdc.Lsn`               |
| `CDCEvent`          | CDC 事件封装          | `org.dbsyncer.connector.sqlserver.model.DMLEvent`        |

## 二、初始化流程

### 2.1 start() 方法执行流程

当监听器启动时，会执行以下初始化步骤：

```84:113:dbsyncer-connector/dbsyncer-connector-sqlserver/src/main/java/org/dbsyncer/connector/sqlserver/cdc/SqlServerListener.java
    @Override
    public void start() {
        try {
            connectLock.lock();
            if (connected) {
                logger.error("SqlServerExtractor is already started");
                return;
            }
            connected = true;
            connect();
            readTables();
            Assert.notEmpty(tables, "No tables available");

            enableDBCDC();
            enableTableCDC();
            readChangeTables();
            readLastLsn();

            worker = new Worker();
            worker.setName(new StringBuilder("cdc-parser-").append(serverName).append("_").append(worker.hashCode()).toString());
            worker.setDaemon(false);
            worker.start();
            LsnPuller.addExtractor(metaId, this);
        } catch (Exception e) {
            close();
            logger.error("启动失败:{}", e.getMessage());
            throw new SqlServerException(e);
        } finally {
            connectLock.unlock();
        }
    }
```

#### 步骤详解

1. **连接数据库** (`connect()`)
   
   - 获取数据库连接实例
   - 读取配置信息（URL、Schema 等）

2. **读取表列表** (`readTables()`)
   
   - 查询指定 Schema 下的所有用户表
   - 根据 `filterTable` 过滤需要监听的表

3. **启用数据库 CDC** (`enableDBCDC()`)
   
   ```sql
   -- 检查数据库是否已启用 CDC
   SELECT is_cdc_enabled FROM sys.databases WHERE name = 'database_name'
   
   -- 如果未启用，执行启用命令
   EXEC sys.sp_cdc_enable_db
   ```
   
   - 需要确保 SQL Server Agent 服务正在运行
   - 启用后等待 3 秒确保生效

4. **启用表 CDC** (`enableTableCDC()`)
   
   ```sql
   -- 为每个表启用 CDC
   EXEC sys.sp_cdc_enable_table 
       @source_schema = N'schema_name', 
       @source_name = N'table_name', 
       @role_name = NULL, 
       @supports_net_changes = 0
   ```

5. **读取变更表信息** (`readChangeTables()`)
   
   - 执行 `sys.sp_cdc_help_change_data_capture` 获取所有已启用 CDC 的表信息
   - 构建 `SqlServerChangeTable` 对象集合，包含：
     - Schema 名称
     - 表名称
     - Capture Instance 名称
     - 变更表对象 ID
     - 起始/结束 LSN
     - 捕获的列信息

6. **读取最后 LSN** (`readLastLsn()`)
   
   - 从快照（snapshot）中恢复上次处理的 LSN 位置
   - 如果快照为空，获取当前数据库的最大 LSN 作为起始点

7. **启动 Worker 线程**
   
   - 创建并启动工作线程，线程名格式：`cdc-parser-{serverName}_{hashCode}`
   - 设置为非守护线程，确保主程序退出时线程继续运行

8. **注册到 LsnPuller**
   
   - 将当前监听器注册到全局 LsnPuller 中，开始接收 LSN 更新通知

## 三、LSN 轮询机制

### 3.1 LsnPuller 架构

`LsnPuller` 采用**单例模式**，全局只有一个实例，负责轮询所有注册的监听器：

```59:88:dbsyncer-connector/dbsyncer-connector-sqlserver/src/main/java/org/dbsyncer/connector/sqlserver/cdc/LsnPuller.java
    final class Worker extends Thread {

        @Override
        public void run() {
            while (!isInterrupted()) {
                try {
                    if (map.isEmpty()) {
                        TimeUnit.SECONDS.sleep(1);
                        continue;
                    }
                    Lsn maxLsn = null;
                    for (SqlServerListener listener : map.values()) {
                        maxLsn = listener.getMaxLsn();
                        if (null != maxLsn && maxLsn.isAvailable() && maxLsn.compareTo(listener.getLastLsn()) > 0) {
                            listener.pushStopLsn(maxLsn);
                        }
                    }
                    TimeUnit.MILLISECONDS.sleep(DEFAULT_POLL_INTERVAL_MILLIS);
                } catch (Exception e) {
                    logger.error("异常", e);
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException ex) {
                        logger.warn(ex.getMessage());
                    }
                }
            }
        }

    }
```

### 3.2 轮询流程

1. **轮询间隔**：每 100 毫秒（`DEFAULT_POLL_INTERVAL_MILLIS = 100`）执行一次
2. **获取最大 LSN**：对每个注册的监听器调用 `getMaxLsn()` 获取当前数据库的最大 LSN
3. **比较 LSN**：如果最大 LSN 大于监听器最后处理的 LSN，说明有新的变更
4. **推送 LSN**：调用 `pushStopLsn(maxLsn)` 将新的 LSN 推送到监听器的缓冲队列

### 3.3 LSN 推送机制

```402:421:dbsyncer-connector/dbsyncer-connector-sqlserver/src/main/java/org/dbsyncer/connector/sqlserver/cdc/SqlServerListener.java
    public void pushStopLsn(Lsn stopLsn) {
        if (buffer.contains(stopLsn)) {
            return;
        }
        if (!buffer.offer(stopLsn)) {
            try {
                lock.lock();
                while (!buffer.offer(stopLsn) && connected) {
                    logger.warn("[{}]缓存队列容量已达上限[{}], 正在阻塞重试.", this.getClass().getSimpleName(), BUFFER_CAPACITY);
                    try {
                        this.isFull.await(pollInterval.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }
```

**关键特性：**

- 使用 `BlockingQueue<Lsn>` 作为缓冲队列，容量为 256
- 如果队列已满，采用阻塞等待策略，避免丢失 LSN
- 去重机制：如果队列中已存在相同的 LSN，则跳过

## 四、变更数据处理

### 4.1 Worker 线程处理流程

每个 `SqlServerListener` 都有一个独立的 Worker 线程，负责从缓冲队列中取出 LSN 并处理变更数据：

```367:396:dbsyncer-connector/dbsyncer-connector-sqlserver/src/main/java/org/dbsyncer/connector/sqlserver/cdc/SqlServerListener.java
    final class Worker extends Thread {

        @Override
        public void run() {
            while (!isInterrupted() && connected) {
                try {
                    Lsn stopLsn = buffer.take();
                    Lsn poll;
                    while ((poll = buffer.poll()) != null) {
                        stopLsn = poll;
                    }
                    if (!stopLsn.isAvailable() || stopLsn.compareTo(lastLsn) <= 0) {
                        continue;
                    }

                    pull(stopLsn);

                    lastLsn = stopLsn;
                    snapshot.put(LSN_POSITION, lastLsn.toString());
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    if (connected) {
                        logger.error(e.getMessage(), e);
                        sleepInMills(1000L);
                    }
                }
            }
        }
    }
```

**处理步骤：**

1. **阻塞获取 LSN**：使用 `buffer.take()` 阻塞等待队列中的 LSN
2. **合并多个 LSN**：如果队列中有多个 LSN，取最新的作为 `stopLsn`（批量合并优化）
3. **验证 LSN**：检查 LSN 是否有效且大于最后处理的 LSN
4. **拉取变更数据**：调用 `pull(stopLsn)` 查询并处理变更
5. **更新位置**：更新 `lastLsn` 并保存到快照中，用于断点续传

### 4.2 数据拉取（pull 方法）

`pull()` 方法是核心的数据查询逻辑：

```243:273:dbsyncer-connector/dbsyncer-connector-sqlserver/src/main/java/org/dbsyncer/connector/sqlserver/cdc/SqlServerListener.java
    private void pull(Lsn stopLsn) {
        Lsn startLsn = queryAndMap(GET_INCREMENT_LSN, statement -> statement.setBytes(1, lastLsn.getBinary()), rs -> Lsn.valueOf(rs.getBytes(1)));
        changeTables.forEach(changeTable -> {
            final String query = GET_ALL_CHANGES_FOR_TABLE.replace(STATEMENTS_PLACEHOLDER, changeTable.getCaptureInstance());
            List<CDCEvent> list = queryAndMapList(query, statement -> {
                statement.setBytes(1, startLsn.getBinary());
                statement.setBytes(2, stopLsn.getBinary());
            }, rs -> {
                int columnCount = rs.getMetaData().getColumnCount();
                List<Object> row = null;
                List<CDCEvent> data = new ArrayList<>();
                while (rs.next()) {
                    // skip update before
                    final int operation = rs.getInt(3);
                    if (TableOperationEnum.isUpdateBefore(operation)) {
                        continue;
                    }
                    row = new ArrayList<>(columnCount - OFFSET_COLUMNS);
                    for (int i = OFFSET_COLUMNS + 1; i <= columnCount; i++) {
                        row.add(rs.getObject(i));
                    }
                    data.add(new CDCEvent(changeTable.getTableName(), operation, row));
                }
                return data;
            });

            if (!CollectionUtils.isEmpty(list)) {
                parseEvent(list, stopLsn);
            }
        });
    }
```

**关键 SQL 查询：**

1. **获取起始 LSN**：
   
   ```sql
   SELECT sys.fn_cdc_increment_lsn(?)
   ```
   
   - 参数：`lastLsn`
   - 返回：大于 `lastLsn` 的最小有效 LSN
   - **作用**：确保不遗漏任何变更，即使 LSN 不连续

2. **查询变更数据**：
   
   ```sql
   SELECT * FROM cdc.[fn_cdc_get_all_changes_{capture_instance}](?, ?, N'all update old')
   ORDER BY [__$start_lsn] ASC, [__$seqval] ASC
   ```
   
   - 参数 1：`startLsn`（起始 LSN）
   - 参数 2：`stopLsn`（结束 LSN）
   - 参数 3：`'all update old'`（返回所有变更，包括 UPDATE 的前后值）
   - **排序**：按 LSN 和序列值升序排列，保证事件顺序

**数据处理逻辑：**

1. **跳过 UPDATE_BEFORE**：UPDATE 操作会产生两条记录（before 和 after），只保留 after
2. **提取数据列**：跳过前 4 个系统列（`__$start_lsn`, `__$end_lsn`, `__$seqval`, `__$operation`），提取实际数据列
3. **构建 CDCEvent**：封装表名、操作类型和行数据

### 4.3 事件解析（parseEvent 方法）

将 CDC 事件转换为系统内部事件格式：

```290:309:dbsyncer-connector/dbsyncer-connector-sqlserver/src/main/java/org/dbsyncer/connector/sqlserver/cdc/SqlServerListener.java
    private void parseEvent(List<CDCEvent> list, Lsn stopLsn) {
        int size = list.size();
        for (int i = 0; i < size; i++) {
            boolean isEnd = i == size - 1;
            CDCEvent event = list.get(i);
            if (TableOperationEnum.isUpdateAfter(event.getCode())) {
                trySendEvent(new RowChangedEvent(event.getTableName(), ConnectorConstant.OPERTION_UPDATE, event.getRow(), null, (isEnd ? stopLsn : null)));
                continue;
            }

            if (TableOperationEnum.isInsert(event.getCode())) {
                trySendEvent(new RowChangedEvent(event.getTableName(), ConnectorConstant.OPERTION_INSERT, event.getRow(), null, (isEnd ? stopLsn : null)));
                continue;
            }

            if (TableOperationEnum.isDelete(event.getCode())) {
                trySendEvent(new RowChangedEvent(event.getTableName(), ConnectorConstant.OPERTION_DELETE, event.getRow(), null, (isEnd ? stopLsn : null)));
            }
        }
    }
```

**事件类型映射：**

| CDC 操作码 | 操作类型          | 系统事件类型            |
| ------- | ------------- | ----------------- |
| 1       | DELETE        | `OPERTION_DELETE` |
| 2       | INSERT        | `OPERTION_INSERT` |
| 3       | UPDATE_BEFORE | 跳过（不处理）           |
| 4       | UPDATE_AFTER  | `OPERTION_UPDATE` |

**LSN 位置标记：**

- 只有最后一个事件会携带 `stopLsn` 作为位置标记
- 用于断点续传和位置追踪

## 五、事件发送机制

### 5.1 事件发送（trySendEvent）

```275:288:dbsyncer-connector/dbsyncer-connector-sqlserver/src/main/java/org/dbsyncer/connector/sqlserver/cdc/SqlServerListener.java
    private void trySendEvent(RowChangedEvent event){
        while (connected){
            try {
                sendChangedEvent(event);
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

**特性：**

- **阻塞重试**：如果事件队列已满（`QueueOverflowException`），等待 1 毫秒后重试
- **确保送达**：在连接状态下持续重试，直到事件成功发送

### 5.2 事件流转路径

事件发送的完整调用链：

```
trySendEvent()
  ↓
sendChangedEvent()  (AbstractDatabaseListener)
  ↓
changeEvent()  (AbstractListener)
  ↓
watcher.changeEvent()  (Watcher 接口实现)
  ↓
上层事件处理器（同步到目标数据库）
```

**事件对象结构：**

- `RowChangedEvent`：包含表名、操作类型、变更行数据、位置信息等

## 六、数据流程图

```
┌─────────────────────────────────────────────────────────────┐
│                    SQL Server Database                      │
│                  (CDC Enabled Tables)                       │
│                                                             │
│  数据变更 → CDC 捕获 → 写入 cdc.change_tables              │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            │ SQL Server Agent 捕获变更
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    LsnPuller (单例)                        │
│                    Worker Thread                            │
│                                                             │
│  每 100ms 轮询:                                             │
│  1. 获取 maxLsn = sys.fn_cdc_get_max_lsn()                 │
│  2. 比较 maxLsn > listener.getLastLsn()                    │
│  3. listener.pushStopLsn(maxLsn)                           │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            │ pushStopLsn()
                            ▼
┌─────────────────────────────────────────────────────────────┐
│              SqlServerListener.Buffer Queue                 │
│         BlockingQueue<Lsn> (容量: 256)                      │
│                                                             │
│  缓冲 LSN，支持背压控制                                       │
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
│     - 计算 startLsn = fn_cdc_increment_lsn(lastLsn)         │
│     - 查询 cdc.fn_cdc_get_all_changes_xxx()                │
│     - 解析 CDCEvent                                         │
│     - parseEvent() → trySendEvent()                          │
│  4. 更新 lastLsn 并保存到 snapshot                           │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            │ sendChangedEvent()
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    AbstractListener                         │
│                                                             │
│  changeEvent() → watcher.changeEvent()                      │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            │ 事件分发
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Watcher (上层处理器)                      │
│                                                             │
│  处理事件并同步到目标数据库                                   │
└─────────────────────────────────────────────────────────────┘
```

## 七、关键设计点

### 7.1 双线程架构

- **LsnPuller.Worker**：轻量级轮询线程，只负责获取 LSN
- **SqlServerListener.Worker**：重量级处理线程，负责查询和处理数据

**优势：**

- 解耦轮询和处理逻辑
- 支持多个监听器共享同一个 LsnPuller
- 避免阻塞，提高响应速度

### 7.2 缓冲队列机制

- 使用 `BlockingQueue<Lsn>` 作为缓冲，容量 256
- 支持背压控制：队列满时阻塞等待
- LSN 去重：避免重复处理

### 7.3 LSN 增量查询

使用 `sys.fn_cdc_increment_lsn()` 确保：

- 即使 LSN 不连续，也不会遗漏变更
- 处理 LSN 跳跃的情况（如数据库重启）

### 7.4 快照持久化

- 将最后处理的 LSN 保存到 `snapshot` 中
- 支持断点续传：重启后从上次位置继续
- 定期刷新到持久化存储（通过 `flushEvent()`）

### 7.5 异常处理

- **队列溢出**：阻塞重试，确保事件不丢失
- **查询异常**：休眠 1 秒后继续处理
- **连接中断**：优雅退出，清理资源

## 八、性能优化

### 8.1 批量处理

- Worker 线程会合并队列中的多个 LSN，取最新的作为 `stopLsn`
- 减少数据库查询次数

### 8.2 轮询间隔

- LsnPuller 轮询间隔：100 毫秒
- 平衡实时性和系统负载

### 8.3 事件过滤

- 跳过 UPDATE_BEFORE 事件，只处理 UPDATE_AFTER
- 减少不必要的事件处理

## 九、注意事项

### 9.1 SQL Server Agent

- **必须运行**：CDC 功能依赖 SQL Server Agent 服务
- 如果 Agent 未运行，CDC 无法捕获变更

### 9.2 CDC 启用

- 数据库级别：需要执行 `sys.sp_cdc_enable_db`
- 表级别：需要为每个表执行 `sys.sp_cdc_enable_table`
- 启用后需要等待几秒确保生效

### 9.3 LSN 范围

- 使用 `fn_cdc_increment_lsn()` 获取起始 LSN，避免遗漏
- 查询范围：`[startLsn, stopLsn]`，左闭右闭区间

### 9.4 事件顺序

- CDC 查询结果按 `__$start_lsn` 和 `__$seqval` 排序
- 保证事件按时间顺序处理

### 9.5 资源清理

- 监听器关闭时，需要从 LsnPuller 中移除
- 中断 Worker 线程，释放数据库连接

## 十、相关代码文件

| 文件路径                         | 说明       |
| ---------------------------- | -------- |
| `SqlServerListener.java`     | 监听器主类    |
| `LsnPuller.java`             | LSN 轮询器  |
| `Lsn.java`                   | LSN 值对象  |
| `CDCEvent.java`              | CDC 事件封装 |
| `SqlServerChangeTable.java`  | 变更表信息模型  |
| `ChangeDataCaptureTest.java` | 测试用例     |

## 十一、参考文档

- [SQL Server CDC 官方文档](https://learn.microsoft.com/zh-cn/sql/relational-databases/track-changes/about-change-data-capture-sql-server)
- [fn_cdc_get_all_changes 函数文档](https://learn.microsoft.com/zh-cn/previous-versions/sql/sql-server-2008/bb510627(v=sql.100))
