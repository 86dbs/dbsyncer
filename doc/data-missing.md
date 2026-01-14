# 数据丢失问题分析与解决方案

## 1. 问题描述

### 1.1 问题现象
在应用反复启停过程中，出现数据丢失问题：

## 2. 根本原因分析

### 2.1 当前架构

#### 2.1.1 队列架构
```
┌─────────────────────────────────────────────────────────┐
│              BufferActuatorRouter                        │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │  GeneralBufferActuator (全局共享)                │   │
│  │  - 处理所有 meta 的 DDL 事件                    │   │
│  │  - 处理未绑定表的事件                            │   │
│  └──────────────────────────────────────────────────┘   │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │  router: Map<metaId, Map<tableName, Actuator>>  │   │
│  │                                                   │   │
│  │  meta1:                                          │   │
│  │    ├─ TableGroupBufferActuator-A (表A)          │   │
│  │    └─ TableGroupBufferActuator-B (表B)          │   │
│  │                                                   │   │
│  │  meta2:                                          │   │
│  │    ├─ TableGroupBufferActuator-C (表C)          │   │
│  │    └─ TableGroupBufferActuator-D (表D)          │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

#### 2.1.2 事件路由逻辑
```java
// BufferActuatorRouter.execute()
public void execute(String metaId, ChangedEvent event) {
    router.compute(metaId, (k, processor) -> {
        if (processor == null) {
            // 未绑定表组 → GeneralBufferActuator (全局共享)
            offer(generalBufferActuator, event);
            return null;
        }
        
        processor.compute(event.getSourceTableName(), (x, actuator) -> {
            if (actuator == null) {
                // 表未绑定 → GeneralBufferActuator (全局共享)
                offer(generalBufferActuator, event);
                return null;
            }
            // 表已绑定 → TableGroupBufferActuator (该 meta 独享)
            offer(actuator, event);
            return actuator;
        });
    });
}
```

### 2.2 核心问题

#### 2.2.1 多队列并发处理
- **问题1：GeneralBufferActuator 全局共享**
  - 所有 meta 的 DDL 和未绑定表事件都进入同一个队列
  - 不同 meta 的事件混在一起，无法保证顺序

- **问题2：同一 meta 可能有多个队列**
  - 已绑定表的事件 → `TableGroupBufferActuator`（每个表一个队列）
  - 未绑定表的事件 → `GeneralBufferActuator`（全局共享）
  - 多个队列并发处理，批次顺序不确定

- **问题3：批次处理顺序不确定**
  - 每个 `AbstractBufferActuator` 都有独立的定时任务
  - 不同队列的 `process()` 可能同时执行
  - `ConcurrentHashMap` 的迭代顺序不确定

#### 2.2.2 快照更新时机问题

**当前实现：**
```java
public void refreshOffset(ChangedOffset offset) {
    listener.refreshEvent(offset);
    
    // 检查所有队列是否为空
    long queueSize = getQueueSize().get();
    if (queueSize == 0) {
        hasPendingTask = false;
    }
}
```

**问题场景：**
```
时间线：
T1: 表A事件（位置100）进入 TableGroupBufferActuator-A 队列
T2: 表B事件（位置200）进入 TableGroupBufferActuator-B 队列
T3: TableGroupBufferActuator-B 先处理完成，调用 refreshOffset(位置200)
T4: 此时 getQueueSize() 检查发现 TableGroupBufferActuator-A 队列还有事件
T5: 不更新快照，继续等待
T6: 应用停止，TableGroupBufferActuator-A 队列中的事件丢失
T7: 重启后从位置200恢复，跳过位置100的事件
```

**如果改为 process() 完成后立即更新：**
```
时间线：
T1: 表A事件（位置100）进入 TableGroupBufferActuator-A 队列
T2: 表B事件（位置200）进入 TableGroupBufferActuator-B 队列
T3: TableGroupBufferActuator-B 的 process() 完成，立即更新快照到位置200
T4: TableGroupBufferActuator-A 还在处理位置100的事件
T5: 应用停止，TableGroupBufferActuator-A 队列中的事件丢失
T6: 重启后从位置200恢复，跳过位置100的事件
```

### 2.3 问题根源总结

1. **多队列并发处理**：同一 meta 的事件可能进入多个队列，无法保证顺序
2. **全局共享队列**：`GeneralBufferActuator` 被所有 meta 共享，事件混在一起
3. **批次顺序不确定**：多个队列的批次处理顺序不确定，可能导致快照跳过未处理的事件
4. **快照更新时机**：必须在所有队列都为空时才能更新快照，但多队列架构使得这个检查变得复杂

## 3. 解决方案

### 3.1 方案概述

**核心思想：每个 meta 独享一个队列**

- 每个 meta 创建独立的执行器（类似 `GeneralBufferActuator`）
- 所有该 meta 的事件（包括 DDL 和所有表）都进入同一个队列
- 队列内事件按 binlog 顺序（FIFO）处理
- `process()` 完成后，由于队列是 FIFO，队列中的事件位置一定 >= 当前批次的最大位置，所以可以直接更新快照，不需要等待队列为空

### 3.2 架构设计

#### 3.2.1 新架构
```
┌─────────────────────────────────────────────────────────┐
│              BufferActuatorRouter                        │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │  metaActuatorMap: Map<metaId, MetaBufferActuator>│   │
│  │                                                   │   │
│  │  meta1:                                          │   │
│  │    └─ MetaBufferActuator-1 (独享队列)           │   │
│  │       - 处理 meta1 的所有事件                   │   │
│  │       - DDL、表A、表B 都进入此队列               │   │
│  │                                                   │   │
│  │  meta2:                                          │   │
│  │    └─ MetaBufferActuator-2 (独享队列)           │   │
│  │       - 处理 meta2 的所有事件                   │   │
│  │       - DDL、表C、表D 都进入此队列               │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

#### 3.2.2 优势

1. **顺序保证**
   - 队列是 FIFO，事件按 binlog 顺序进入
   - 处理顺序与 binlog 顺序一致

2. **快照更新简化**
   - `process()` 完成后，传入的 offset 是当前批次的最大位置
   - 由于队列是 FIFO，队列中的事件位置一定 >= 当前批次的最大位置
   - 所以可以直接更新快照，不需要等待队列为空
   - 不会跳过位置更早的事件（因为都在同一个队列中，且按顺序处理）

3. **代码简化**
   - `refreshOffset()` 只需要检查该 meta 的队列是否为空
   - 不需要全局的 `getQueueSize()` 检查
   - 不需要考虑多队列并发问题

4. **隔离性**
   - 不同 meta 的事件完全隔离
   - 一个 meta 的问题不影响其他 meta

### 3.3 实现步骤

#### 3.3.1 创建 MetaBufferActuator

创建一个新的执行器，每个 meta 一个实例：

**关键实现点：**

1. **依赖注入问题**：`MetaBufferActuator` 不是 Spring Bean，无法使用 `@Resource` 自动注入。需要通过 `ApplicationContext` 手动注入，或通过构造函数/方法参数传递。

2. **线程池共享**：多个 `MetaBufferActuator` 共享 `generalExecutor` 线程池，这是可以接受的，因为：
   - 线程池本身就是为了并发执行任务而设计的
   - 不同 meta 的任务可以并发执行，提高吞吐量
   - 如果需要隔离，可以为每个 meta 创建独立的线程池（但会增加资源消耗）

**实现示例：**

```java
/**
 * Meta 专用执行器（每个 meta 独享一个队列）
 */
public class MetaBufferActuator extends AbstractBufferActuator<WriterRequest, WriterResponse> {
    private String metaId;
    
    // 通过构造函数或 setter 方法注入依赖
    private GeneralBufferConfig generalBufferConfig;
    private Executor generalExecutor;
    private ConnectorFactory connectorFactory;
    private ProfileComponent profileComponent;
    private PluginFactory pluginFactory;
    private FlushStrategy flushStrategy;
    private DDLParser ddlParser;
    private TableGroupContext tableGroupContext;
    private LogService logService;
    
    public void buildConfig() {
        setConfig(generalBufferConfig);
        super.buildConfig();
    }
    
    @Override
    protected String getPartitionKey(WriterRequest request) {
        return request.getTableName();
    }
    
    @Override
    protected void partition(WriterRequest request, WriterResponse response) {
        // 实现类似 GeneralBufferActuator.partition()
    }
    
    @Override
    protected boolean skipPartition(WriterRequest nextRequest, WriterResponse response) {
        // 实现类似 GeneralBufferActuator.skipPartition()
    }
    
    @Override
    public void pull(WriterResponse response) {
        // 实现类似 GeneralBufferActuator.pull()
    }
    
    @Override
    protected void offerFailed(BlockingQueue<WriterRequest> queue, WriterRequest request) {
        throw new QueueOverflowException("缓存队列已满");
    }
    
    @Override
    protected void meter(TimeRegistry timeRegistry, long count) {
        // 统计该 meta 的执行器同步效率TPS
        timeRegistry.meter(TimeRegistry.GENERAL_BUFFER_ACTUATOR_TPS).add(count);
    }
    
    @Override
    public Executor getExecutor() {
        return generalExecutor;
    }
    
    // getter/setter 方法
    public void setMetaId(String metaId) {
        this.metaId = metaId;
    }
    
    public void setGeneralBufferConfig(GeneralBufferConfig generalBufferConfig) {
        this.generalBufferConfig = generalBufferConfig;
    }
    
    public void setGeneralExecutor(Executor generalExecutor) {
        this.generalExecutor = generalExecutor;
    }
    
    // ... 其他 setter 方法
}
```

#### 3.3.2 修改 BufferActuatorRouter

**修改前：**
```java
@Resource
private GeneralBufferActuator generalBufferActuator;

private final Map<String, Map<String, TableGroupBufferActuator>> router = new ConcurrentHashMap<>();
```

**修改后：**
```java
// 移除全局 GeneralBufferActuator
// private GeneralBufferActuator generalBufferActuator;

// 每个 meta 一个执行器
private final Map<String, MetaBufferActuator> metaActuatorMap = new ConcurrentHashMap<>();
```

#### 3.3.3 修改事件路由逻辑

**修改前：**
```java
public void execute(String metaId, ChangedEvent event) {
    router.compute(metaId, (k, processor) -> {
        if (processor == null) {
            offer(generalBufferActuator, event);  // 全局共享
            return null;
        }
        processor.compute(event.getSourceTableName(), (x, actuator) -> {
            if (actuator == null) {
                offer(generalBufferActuator, event);  // 全局共享
                return null;
            }
            offer(actuator, event);
            return actuator;
        });
    });
}
```

**修改后：**
```java
public void execute(String metaId, ChangedEvent event) {
    // 获取或创建该 meta 的执行器
    MetaBufferActuator actuator = metaActuatorMap.computeIfAbsent(metaId, k -> {
        MetaBufferActuator newActuator = createMetaActuator(metaId);
        newActuator.buildConfig();
        return newActuator;
    });
    
    // 所有事件都进入该 meta 的队列
    offer(actuator, event);
}
```

#### 3.3.4 修改快照更新逻辑

**修改前：**
```java
public void refreshOffset(ChangedOffset offset) {
    listener.refreshEvent(offset);
    
    // 检查所有队列是否为空
    long queueSize = getQueueSize().get();
    if (queueSize == 0) {
        hasPendingTask = false;
    }
}
```

**修改后：**
```java
public void refreshOffset(ChangedOffset offset) {
    Meta meta = profileComponent.getMeta(offset.getMetaId());
    Listener listener = meta.getListener();
    
    // 每个 meta 独享一个队列，队列是 FIFO，事件按 binlog 顺序进入
    // process() 传入的 offset 是当前批次的最大位置
    // 队列中的事件位置一定 >= 当前批次的最大位置
    // 所以更新快照到当前批次的最大位置是安全的，不需要等待队列为空
    listener.refreshEvent(offset);
    
    // 如果队列为空，清除该 meta 的 pending 状态
    MetaBufferActuator actuator = metaActuatorMap.get(offset.getMetaId());
    if (actuator != null && actuator.getQueue().isEmpty()) {
        // 检查所有 meta 的队列是否都为空
        boolean allEmpty = metaActuatorMap.values().stream()
            .allMatch(a -> a.getQueue().isEmpty());
        if (allEmpty) {
            hasPendingTask = false;
            if (logger.isDebugEnabled()) {
                logger.debug("---- finished sync, all meta queues are empty, cleared pending task");
            }
        }
    }
}
```

#### 3.3.5 修改 bind/unbind 逻辑

**修改前：**
```java
public void bind(String metaId, List<TableGroup> tableGroups) {
    router.computeIfAbsent(metaId, k -> {
        Map<String, TableGroupBufferActuator> processor = new ConcurrentHashMap<>();
        // 为每个表创建 TableGroupBufferActuator
        for (TableGroup tableGroup : tableGroups) {
            // ...
        }
        return processor;
    });
}
```

**修改后：**
```java
@Resource
private GeneralBufferConfig generalBufferConfig;

@Resource
private Executor generalExecutor;

@Resource
private ConnectorFactory connectorFactory;

@Resource
private ProfileComponent profileComponent;

@Resource
private PluginFactory pluginFactory;

@Resource
private FlushStrategy flushStrategy;

@Resource
private DDLParser ddlParser;

@Resource
private TableGroupContext tableGroupContext;

@Resource
private LogService logService;

public void bind(String metaId, List<TableGroup> tableGroups) {
    // 创建或获取该 meta 的执行器
    metaActuatorMap.computeIfAbsent(metaId, k -> {
        MetaBufferActuator actuator = createMetaActuator(metaId);
        actuator.buildConfig();
        return actuator;
    });
    
    // 不再需要为每个表创建执行器
    // 所有事件都进入该 meta 的执行器
}

private MetaBufferActuator createMetaActuator(String metaId) {
    MetaBufferActuator actuator = new MetaBufferActuator();
    actuator.setMetaId(metaId);
    // 手动注入所有依赖
    actuator.setGeneralBufferConfig(generalBufferConfig);
    actuator.setGeneralExecutor(generalExecutor);
    actuator.setConnectorFactory(connectorFactory);
    actuator.setProfileComponent(profileComponent);
    actuator.setPluginFactory(pluginFactory);
    actuator.setFlushStrategy(flushStrategy);
    actuator.setDdlParser(ddlParser);
    actuator.setTableGroupContext(tableGroupContext);
    actuator.setLogService(logService);
    return actuator;
}

public void unbind(String metaId) {
    metaActuatorMap.computeIfPresent(metaId, (k, actuator) -> {
        // 停止定时任务
        scheduledTaskService.stop(actuator.getTaskKey());
        // 返回 null 表示移除
        return null;
    });
    
    // 检查所有 meta 的队列是否都为空
    boolean allEmpty = metaActuatorMap.values().stream()
        .allMatch(a -> a.getQueue().isEmpty());
    if (allEmpty) {
        hasPendingTask = false;
    }
}

@Override
public void destroy() {
    // 停止所有 MetaBufferActuator 的定时任务
    metaActuatorMap.values().forEach(actuator -> {
        scheduledTaskService.stop(actuator.getTaskKey());
    });
    metaActuatorMap.clear();
}

public AtomicLong getQueueSize() {
    AtomicLong total = new AtomicLong();
    // 统计所有 meta 的队列大小
    metaActuatorMap.values().forEach(actuator -> {
        total.addAndGet(actuator.getQueue().size());
    });
    return total;
}
```

### 3.4 关键代码变更清单

1. **新增类**
   - `MetaBufferActuator.java` - Meta 专用执行器（复用 `GeneralBufferConfig`）

2. **修改类**
   - `BufferActuatorRouter.java`
     - 移除 `GeneralBufferActuator` 依赖
     - 移除 `router` Map（不再需要按表路由）
     - 新增 `metaActuatorMap` Map
     - 修改 `execute()` 方法
     - 修改 `refreshOffset()` 方法
     - 修改 `bind()` 方法
     - 修改 `unbind()` 方法（停止并移除 MetaBufferActuator）
     - 修改 `getQueueSize()` 方法（按 meta 统计）
     - 修改 `destroy()` 方法（清理所有 MetaBufferActuator）
     - 新增 `createMetaActuator()` 工厂方法（注入依赖）
   - `MetaBufferActuator.java`
     - 注入 `GeneralBufferConfig` 配置
     - 实现 `getPartitionKey()` 和 `partition()` 方法
     - 实现 `pull()` 方法（类似 `GeneralBufferActuator`）

3. **配置变更**
   - 复用 `GeneralBufferConfig`，无需新增配置
   - 使用现有 `dbsyncer.parser.general.*` 配置
   - 所有 meta 共享相同的配置

4. **废弃类**
   - `GeneralBufferActuator.java` - 如果不再需要，可以废弃
   - `TableGroupBufferActuator.java` - 如果不再需要，可以废弃
   - `TableGroupBufferConfig.java` - 如果不再需要，可以废弃

5. **废弃配置项**
   - `dbsyncer.parser.table.group.*` 所有配置项（不再需要 TableGroupBufferActuator）：
     - `dbsyncer.parser.table.group.thread-core-size`
     - `dbsyncer.parser.table.group.max-thread-size`
     - `dbsyncer.parser.table.group.thread-queue-capacity`
     - `dbsyncer.parser.table.group.buffer-writer-count`
     - `dbsyncer.parser.table.group.buffer-pull-count`
     - `dbsyncer.parser.table.group.buffer-queue-capacity`
     - `dbsyncer.parser.table.group.buffer-period-millisecond`
   - `SystemConfig.maxBufferActuatorSize`（废弃）：
     - 在新架构下，每个 meta 只有一个执行器，不再需要限制每个 meta 的执行器数量
     - 废弃此配置，移除相关代码和 UI

## 4. 风险评估


### 4.1 配置项处理方案

#### 复用 GeneralBufferConfig

**优点：**
- 代码改动最小
- 配置兼容性好
- 所有 meta 使用相同的配置
- 无需新增配置类

**实现：**
```java
// MetaBufferActuator 复用 GeneralBufferConfig
@Resource
private GeneralBufferConfig generalBufferConfig;

public void buildConfig() {
    setConfig(generalBufferConfig);
    super.buildConfig();
}
```

**配置示例：**
```properties
# application.properties
# 使用现有的 dbsyncer.parser.general.* 配置
dbsyncer.parser.general.buffer-writer-count=100
dbsyncer.parser.general.buffer-pull-count=1000
dbsyncer.parser.general.buffer-queue-capacity=30000
dbsyncer.parser.general.buffer-period-millisecond=300
dbsyncer.parser.general.thread-core-size=4
dbsyncer.parser.general.max-thread-size=8
dbsyncer.parser.general.thread-queue-capacity=1000
```

#### 配置项说明

| 配置项 | 说明 | 默认值 | 建议值 |
|--------|------|--------|--------|
| `buffer-writer-count` | 单次执行任务数 | 100 | 根据数据量调整 |
| `buffer-pull-count` | 每次消费缓存队列的任务数 | 1000 | 根据数据量调整 |
| `buffer-queue-capacity` | 缓存队列容量 | 30000 | 根据内存和并发量调整 |
| `buffer-period-millisecond` | 定时消费缓存队列间隔(毫秒) | 300 | 根据实时性要求调整 |
| `thread-core-size` | 工作线程数 | CPU核心数*2 | 根据并发量调整 |
| `max-thread-size` | 最大工作线程数 | CPU核心数*2 | 根据并发量调整 |
| `thread-queue-capacity` | 工作线任务队列容量 | 1000 | 根据并发量调整 |

#### 配置迁移建议

1. **兼容性处理**
   - 现有 `dbsyncer.parser.general.*` 配置可以继续使用，无需修改
   - 所有 meta 共享相同的配置，简化配置管理

2. **配置说明**
   - 所有配置项都使用 `dbsyncer.parser.general.*` 前缀
   - 配置对所有 meta 生效
   - 如需调整，修改 `dbsyncer.parser.general.*` 配置即可

4. **废弃配置项迁移**
   - **`dbsyncer.parser.table.group.*` 配置项**：
     - 这些配置项在新架构下不再使用，可以删除
     - 如果之前使用了这些配置，建议迁移到 `dbsyncer.parser.general.*` 配置
     - 迁移示例：
       ```properties
       # 旧配置（废弃）
       dbsyncer.parser.table.group.buffer-writer-count=1000
       dbsyncer.parser.table.group.buffer-pull-count=1000
       dbsyncer.parser.table.group.buffer-queue-capacity=10000
       dbsyncer.parser.table.group.buffer-period-millisecond=300
       dbsyncer.parser.table.group.thread-core-size=1
       dbsyncer.parser.table.group.max-thread-size=1
       dbsyncer.parser.table.group.thread-queue-capacity=16
       
       # 新配置（使用 general 配置）
       dbsyncer.parser.general.buffer-writer-count=1000
       dbsyncer.parser.general.buffer-pull-count=1000
       dbsyncer.parser.general.buffer-queue-capacity=10000
       dbsyncer.parser.general.buffer-period-millisecond=300
       dbsyncer.parser.general.thread-core-size=1
       dbsyncer.parser.general.max-thread-size=1
       dbsyncer.parser.general.thread-queue-capacity=16
       ```
   - **`SystemConfig.maxBufferActuatorSize` 配置**：
     - 在新架构下，每个 meta 只有一个执行器，不再需要限制每个 meta 的执行器数量
     - **选项1（推荐）**：废弃此配置，移除相关代码和 UI
     - **选项2**：保留此配置，但调整其含义为"每个 meta 的最大表数量"（如果系统需要限制）
     - **选项3**：保留此配置，但调整其含义为"系统最大 meta 数量"（如果系统需要限制）

### 4.2 兼容性

**潜在问题：**
- 现有代码可能依赖 `GeneralBufferActuator` 或 `TableGroupBufferActuator`
- 配置可能需要调整

**缓解措施：**
- 保持接口兼容，内部实现改变
- 逐步迁移，先支持新架构，保留旧架构作为备选
- 通过工厂方法手动注入依赖，确保所有依赖正确初始化

### 4.3 关键实现注意事项

#### 4.3.1 依赖注入

**问题**：`MetaBufferActuator` 不是 Spring Bean，无法使用 `@Resource` 自动注入。

**解决方案**：
- 在 `BufferActuatorRouter` 中通过 `@Resource` 注入所有需要的依赖
- 在 `createMetaActuator()` 工厂方法中，手动将这些依赖注入到 `MetaBufferActuator` 实例
- 确保所有依赖都正确传递

#### 4.3.2 线程池共享

**问题**：多个 `MetaBufferActuator` 共享 `generalExecutor` 线程池。

**分析**：
- 线程池本身就是为了并发执行任务而设计的
- 不同 meta 的任务可以并发执行，提高吞吐量
- 共享线程池不会影响数据顺序性（因为每个 meta 的队列是独立的）

**结论**：共享线程池是可以接受的，不需要为每个 meta 创建独立的线程池。

#### 4.3.3 定时任务管理

**问题**：每个 `MetaBufferActuator` 都会创建独立的定时任务。

**解决方案**：
- 在 `buildConfig()` 中调用 `scheduledTaskService.start()` 创建定时任务
- 需要为每个 `MetaBufferActuator` 生成唯一的 `taskKey`
- 在 `unbind()` 和 `destroy()` 中正确停止定时任务，避免资源泄漏

**实现示例：**
```java
// 在 AbstractBufferActuator 中添加 taskKey 字段
private String taskKey;

protected void buildConfig() {
    Assert.notNull(config, "请先配置缓存执行器");
    buildQueueConfig();
    // 生成唯一的 taskKey
    taskKey = UUIDUtil.getUUID();
    scheduledTaskService.start(taskKey, config.getBufferPeriodMillisecond(), this);
}

public String getTaskKey() {
    return taskKey;
}
```

#### 4.3.4 非任务事件处理

**问题**：`MySQLListener` 中的非任务 XID 事件如何处理？

**说明**：
- 非任务事件（如系统表变更）也需要更新快照，以跟踪 binlog 位置
- 这些事件不会进入 `BufferActuatorRouter.execute()`，而是直接在 `MySQLListener` 中处理
- 非任务事件的快照更新逻辑保持不变，不受新架构影响

#### 4.3.5 生命周期管理

**创建时机**：
- 在 `bind()` 方法中创建 `MetaBufferActuator`
- 或者在 `execute()` 方法中延迟创建（如果 meta 还没有绑定）

**销毁时机**：
- 在 `unbind()` 方法中停止并移除 `MetaBufferActuator`
- 在 `destroy()` 方法中清理所有 `MetaBufferActuator`

**注意事项**：
- 确保定时任务正确停止，避免资源泄漏
- 确保队列中的事件处理完成后再销毁





## 6. 预期效果

### 6.1 问题解决
- ✅ 彻底解决数据丢失问题
- ✅ 保证数据顺序性
- ✅ 简化快照更新逻辑

### 6.2 代码质量
- ✅ 代码更简洁
- ✅ 逻辑更清晰
- ✅ 维护更容易

### 6.3 性能
- ✅ 性能影响可控
- ✅ 内存占用合理
- ✅ 扩展性良好

## 7. 附录

### 7.1 相关文件
- `dbsyncer-parser/src/main/java/org/dbsyncer/parser/flush/impl/BufferActuatorRouter.java`
- `dbsyncer-parser/src/main/java/org/dbsyncer/parser/flush/AbstractBufferActuator.java`
- `dbsyncer-parser/src/main/java/org/dbsyncer/parser/flush/impl/GeneralBufferActuator.java`
- `dbsyncer-parser/src/main/java/org/dbsyncer/parser/flush/impl/TableGroupBufferActuator.java`

### 7.2 参考文档
- Binlog 事件顺序：`TABLE_MAP > WRITE_ROWS > UPDATE_ROWS > DELETE_ROWS > XID`
- 快照更新机制：`AbstractListener.refreshEvent()`
- 批次处理机制：`AbstractBufferActuator.process()`
