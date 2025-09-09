# DBSyncer 全量+增量混合模式分析

## 1. 概述

DBSyncer目前提供独立的"全量"和"增量"两种数据同步模式，为了满足更广泛的业务场景需求，需要实现全量+增量的混合同步模式。本文档详细分析实现该混合模式所需的技术改造工作。

## 2. 现状分析

### 2.1 当前同步模式架构

#### 全量同步模式（Full Synchronization）
- **实现类**：`FullPuller`
- **核心特性**：
  - 使用`Task`类跟踪同步进度
  - 通过`ParserEnum.PAGE_INDEX`、`ParserEnum.CURSOR`、`ParserEnum.TABLE_GROUP_INDEX`管理分页状态
  - 在`Meta.snapshot`中保存进度信息
  - 支持断点续传，从上次中断位置恢复
  - 适合初始数据迁移或大批量数据同步

#### 增量同步模式（Incremental Synchronization）
- **实现类**：`IncrementPuller`
- **核心特性**：
  - 使用`ChangedEvent`表示数据变更事件
  - 通过`Listener`监听数据源变更（binlog、CDC等）
  - 使用`BufferActuatorRouter`处理变更事件
  - 在`Meta.snapshot`中保存偏移量信息
  - 支持近实时数据同步

## 现有恢复机制分析与借鉴

### 2.3 现有独立模式的恢复机制

#### 2.3.1 FullPuller恢复特点
- **断点续传**：利用`Meta.snapshot`中的`pageIndex`、`cursor`、`tableGroupIndex`实现精确断点恢复
- **零配置恢复**：启动时自动从上次中断位置继续，无需额外配置
- **状态隔离**：Task状态独立管理，不干扰其他组件

#### 2.3.2 IncrementPuller恢复特点  
- **Listener状态检查**：通过`meta.getListener() == null`判断是否需要重新创建
- **偏移量恢复**：Listener从`Meta.snapshot`中恢复数据库特定的偏移量信息
- **自动重连**：异常时自动清理并重新建立监听连接

#### 2.3.3 ManagerFactory统一恢复
- **系统启动恢复**：`PreloadTemplate`在系统启动时检查所有`MetaEnum.RUNNING`状态的任务并重新启动
- **事件驱动清理**：通过`ClosedEvent`机制自动将完成的任务状态重置为`READY`
- **异常回滚**：启动失败时自动回滚Meta状态

### 2.4 借鉴现有机制的设计原则

**与现有设计保持一致**：
1. **复用Meta.snapshot机制**：不重新发明轮子，继续使用现有的快照存储
2. **保持事件驱动模式**：利用现有的`ClosedEvent`和`ApplicationListener`机制  
3. **延续零配置理念**：启动时自动检查恢复，无需额外配置
4. **维持状态简单性**：避免复杂的状态机，使用简单的状态判断

## 3. 全量+增量混合模式需求分析

### 3.1 业务场景
- **初始化+实时同步**：首次全量同步历史数据，后续增量同步新变更
- **定期全量+持续增量**：定时全量刷新基础数据，平时增量同步
- **故障恢复**：增量同步异常后，自动切换全量重新同步

### 3.2 技术需求
1. **模式定义**：需要新增`FULL_INCREMENT`混合同步模式
2. **任务协调**：实现全量和增量任务的启停协调
3. **状态同步**：统一管理全量和增量的执行状态
4. **数据一致性**：确保全量到增量切换时的数据一致性
5. **故障处理**：异常情况下的模式切换和恢复机制

## 4. 技术实现方案

### 4.1 枚举扩展

#### 4.1.1 ModelEnum 扩展

```java
public enum ModelEnum {
    FULL("full", "全量"),
    INCREMENT("increment", "增量"),
    FULL_INCREMENT("fullIncrement", "全量+增量");  // 新增混合模式
    
    // 新增判断方法
    public static boolean isFullIncrement(String model) {
        return StringUtil.equals(FULL_INCREMENT.getCode(), model);
    }
    
    public static boolean needFullSync(String model) {
        return isFull(model) || isFullIncrement(model);
    }
    
    public static boolean needIncrementSync(String model) {
        return StringUtil.equals(INCREMENT.getCode(), model) || isFullIncrement(model);
    }
}
```

### 4.2 混合模式Puller设计

#### 4.2.1 FullIncrementPuller 类结构
```java
@Component
public final class FullIncrementPuller extends AbstractPuller implements Puller {
    
    @Resource
    private FullPuller fullPuller;
    
    @Resource
    private IncrementPuller incrementPuller;
    
    @Resource
    private ProfileComponent profileComponent;
    
    @Resource
    private LogService logService;
    
    private final Map<String, SyncState> syncStates = new ConcurrentHashMap<>();
    
    // 混合模式状态枚举（最简化设计）
    enum SyncState {
        FULL_PENDING,     // 全量待执行
        FULL_RUNNING,     // 全量执行中  
        INCREMENT_RUNNING // 增量执行中
    }
}
```

#### 4.2.2 核心协调逻辑
```java
@Override
public void start(Mapping mapping) {
    final String metaId = mapping.getMetaId();
    
    Thread coordinator = new Thread(() -> {
        try {
            // 1. 检查故障恢复（零开销）
            SyncState recoveryState = checkAndRecover(mapping);
            
            // 2. 根据状态直接执行（内联逻辑，简化设计）
            switch (recoveryState) {
                case FULL_PENDING:
                case FULL_RUNNING:
                    // 记录增量起始点并执行全量同步
                    recordIncrementStartPoint(mapping);
                    startFullThenIncrement(mapping);
                    break;
                    
                case INCREMENT_RUNNING:
                    // 直接启动增量（全量已完成）
                    startIncrementSync(mapping);
                    break;
                    
                default:
                    throw new ManagerException("不支持的恢复状态: " + recoveryState);
            }
            
        } catch (Exception e) {
            // 异常驱动：直接发布ClosedEvent，让ManagerFactory自动重置状态
            publishClosedEvent(metaId);
            logger.error("混合同步异常，已发布关闭事件: {}", metaId, e);
        }
    });
    
    coordinator.setName("full-increment-coordinator-" + mapping.getId());
    coordinator.start();
}

// 混合模式核心：先执行全量同步，完成后自动转入增量同步
private void startFullThenIncrement(Mapping mapping) {
    String metaId = mapping.getMetaId();
    syncStates.put(metaId, SyncState.FULL_RUNNING);
    
    // 直接使用原始Mapping，通过overridePuller机制控制行为
    Thread fullSyncThread = new Thread(() -> {
        try {
            // 使用现有的FullPuller，但拦截其完成事件
            runFullSyncAndThen(mapping, () -> {
                logger.info("全量同步完成，转入增量模式: {}", metaId);
                startIncrementSync(mapping);
            });
            
        } catch (Exception e) {
            // 异常时才发布ClosedEvent
            publishClosedEvent(metaId);
            logger.error("全量同步异常: {}", metaId, e);
        }
    });
    
    fullSyncThread.setName("mixed-full-sync-" + mapping.getId());
    fullSyncThread.start();
}

// 核心：运行全量同步并在完成后执行回调
private void runFullSyncAndThen(Mapping mapping, Runnable onComplete) {
    // 这里需要具体实现：
    // 1. 使用FullPuller.start(fullMapping)
    // 2. 监控其完成状态
    // 3. 拦截其publishClosedEvent调用
    // 4. 在完成时调用onComplete.run()
    
    // 这是技术实现的关键点，需要进一步设计
    throw new UnsupportedOperationException("需要具体实现拦截机制");
}

private void recordIncrementStartPoint(Mapping mapping) {
    String metaId = mapping.getMetaId();
    Meta meta = profileComponent.getMeta(metaId);
    
    // 关键优化：检查是否已经记录，避免重复记录
    Map<String, String> snapshot = meta.getSnapshot();
    if (isIncrementStartPointRecorded(snapshot)) {
        logger.info("增量起始点已记录，跳过: {}", metaId);
        return;
    }
    
    // 简化设计：委托给连接器获取当前位置
    ConnectorConfig sourceConfig = getSourceConnectorConfig(mapping);
    ConnectorService connectorService = connectorFactory.getConnectorService(sourceConfig.getConnectorType());
    ConnectorInstance connectorInstance = connectorFactory.connect(sourceConfig);
    
    try {
        // 使用现有的getPosition方法，返回当前位置
        Object currentPosition = connectorService.getPosition(connectorInstance);
        
        if (currentPosition != null) {
            // 保存到受保护的字段
            snapshot.put(PROTECTED_CURRENT_POSITION, String.valueOf(currentPosition));
            snapshot.put(PROTECTED_CONNECTOR_TYPE, sourceConfig.getConnectorType());
            logger.info("已记录增量起始位置: metaId={}, connectorType={}, position={}", 
                       metaId, sourceConfig.getConnectorType(), currentPosition);
        } else {
            // 如果连接器不支持位置获取，使用时间戳
            snapshot.put(PROTECTED_INCREMENT_START_TIME, String.valueOf(System.currentTimeMillis()));
            logger.info("连接器不支持位置获取，使用时间戳: {}", metaId);
        }
        
    } catch (Exception e) {
        // 异常时使用时间戳备用
        logger.warn("获取增量起始位置失败，使用时间戳: {}", e.getMessage());
        snapshot.put(PROTECTED_INCREMENT_START_TIME, String.valueOf(System.currentTimeMillis()));
    } finally {
        // 清理连接资源
        connectorService.disconnect(connectorInstance);
    }
    
    // 记录全量同步开始时间
    snapshot.put("fullSyncStartTime", String.valueOf(System.currentTimeMillis()));
    snapshot.put(PROTECTED_INCREMENT_RECORDED, "true"); // 标记已记录
    
    profileComponent.editConfigModel(meta);
    logger.info("已记录增量同步起始位置: metaId={}", metaId);
}

// 检查是否已记录增量起始点
private boolean isIncrementStartPointRecorded(Map<String, String> snapshot) {
    return "true".equals(snapshot.get(PROTECTED_INCREMENT_RECORDED));
}

// 简化的受保护字段名常量
private static final String PROTECTED_INCREMENT_RECORDED = "_protected_increment_recorded";
private static final String PROTECTED_CURRENT_POSITION = "_protected_current_position";
private static final String PROTECTED_CONNECTOR_TYPE = "_protected_connector_type";
private static final String PROTECTED_INCREMENT_START_TIME = "_protected_increment_start_time";

private void startIncrementSync(Mapping mapping) {
    String metaId = mapping.getMetaId();
    syncStates.put(metaId, SyncState.INCREMENT_RUNNING);
    
    // 关键：恢复受保护的增量起始点
    restoreProtectedIncrementStartPoint(mapping);
    
    // 直接使用原始Mapping启动增量同步
    incrementPuller.start(mapping);
    
    logger.info("增量同步已启动，混合模式进入持续运行状态: {}", metaId);
}

// 恢复受保护的增量起始点到正常字段
private void restoreProtectedIncrementStartPoint(Mapping mapping) {
    Meta meta = profileComponent.getMeta(mapping.getMetaId());
    Map<String, String> snapshot = meta.getSnapshot();
    
    // 检查是否有受保护的字段
    if (!isIncrementStartPointRecorded(snapshot)) {
        logger.warn("未找到受保护的增量起始点，增量同步可能从当前时间开始: {}", mapping.getMetaId());
        return;
    }
    
    // 简化设计：直接恢复位置信息，让各连接器自己解析
    String currentPosition = snapshot.get(PROTECTED_CURRENT_POSITION);
    String connectorType = snapshot.get(PROTECTED_CONNECTOR_TYPE);
    String incrementStartTime = snapshot.get(PROTECTED_INCREMENT_START_TIME);
    
    if (StringUtil.isNotBlank(currentPosition)) {
        // 恢复位置信息到标准字段（供IncrementPuller使用）
        snapshot.put("position", currentPosition);
        logger.info("恢复增量位置: connectorType={}, position={}", connectorType, currentPosition);
    } else if (StringUtil.isNotBlank(incrementStartTime)) {
        // 使用时间戳备用
        snapshot.put("incrementStartTime", incrementStartTime);
        logger.info("恢复增量时间戳: {}", incrementStartTime);
    } else {
        logger.warn("无可用的增量起始信息: {}", mapping.getMetaId());
    }
    
    profileComponent.editConfigModel(meta);
    logger.info("已恢复增量起始点: {}", mapping.getMetaId());
}
```

### 4.3 状态管理增强

#### 4.3.1 Meta 扩展设计
在现有`Meta`类的`snapshot`中新增混合模式专用字段：

```
// 在Meta.snapshot中新增的字段
public static final String FULL_INCREMENT_STATE = "fullIncrementState";
public static final String FULL_START_TIME = "fullStartTime";
public static final String FULL_END_TIME = "fullEndTime";
public static final String INCREMENT_START_TIME = "incrementStartTime";
public static final String LAST_SYNC_CHECKPOINT = "lastSyncCheckpoint";
```

#### 4.3.2 状态持久化
```
private void persistMixedState(String metaId, SyncState state, Map<String, Object> extraData) {
    Meta meta = profileComponent.getMeta(metaId);
    Map<String, String> snapshot = meta.getSnapshot();
    
    snapshot.put(FULL_INCREMENT_STATE, state.name());
    snapshot.put("stateUpdateTime", String.valueOf(System.currentTimeMillis()));
    
    // 保存额外状态数据
    if (extraData != null) {
        extraData.forEach((k, v) -> snapshot.put(k, String.valueOf(v)));
    }
    
    profileComponent.editConfigModel(meta);
}
```

### 4.4 ManagerFactory 改造

#### 4.4.1 Puller 获取逻辑扩展
```
private Puller getPuller(Mapping mapping) {
    Assert.notNull(mapping, "驱动不能为空");
    String model = mapping.getModel();
    
    // 混合模式使用专用的 FullIncrementPuller
    if (ModelEnum.isFullIncrement(model)) {
        return map.get("fullIncrementPuller");
    }
    
    // 原有逻辑保持不变
    String pullerName = model.concat("Puller");
    Puller puller = map.get(pullerName);
    Assert.notNull(puller, String.format("未知的同步方式: %s", model));
    return puller;
}
```

### 4.5 数据一致性保证

#### 4.5.1 关键原则：先记录位置，再执行全量

**核心策略**：在开始全量同步之前，必须先记录增量同步的起始位置。这是确保数据一致性的关键步骤。

```
private void ensureDataConsistency(Mapping mapping) {
    String metaId = mapping.getMetaId();
    
    // 关键：先记录起始位置，再开始全量同步
    // 这样可以确保增量同步能够捕获全量同步期间的所有变更
    recordIncrementStartPoint(mapping);
    
    // 然后执行全量同步
    executeFullSync(mapping);
    
    // 验证一致性检查点
    validateConsistencyCheckpoint(mapping);
}
```

#### 4.5.2 不同数据源的处理策略

```
private void setupIncrementStartPoint(Mapping mapping) {
    Meta meta = profileComponent.getMeta(mapping.getMetaId());
    Map<String, String> snapshot = meta.getSnapshot();
    
    // 从之前记录的起始位置设置增量同步
    ConnectorConfig sourceConfig = getSourceConnectorConfig(mapping);
    String connectorType = sourceConfig.getConnectorType();
    
    switch (connectorType.toLowerCase()) {
        case "mysql":
            // 使用之前记录的binlog位置
            String binlogFile = snapshot.get("binlogFile");
            String binlogPosition = snapshot.get("binlogPosition");
            setupMySQLIncrementFromPosition(snapshot, binlogFile, binlogPosition);
            break;
        case "oracle":
            // 使用之前记录的SCN
            String startSCN = snapshot.get("startSCN");
            setupOracleIncrementFromSCN(snapshot, startSCN);
            break;
        case "sqlserver":
            // 使用之前记录的LSN
            String startLSN = snapshot.get("startLSN");
            setupSQLServerIncrementFromLSN(snapshot, startLSN);
            break;
        case "postgresql":
            // 使用之前记录的WAL位置
            String startWAL = snapshot.get("startWAL");
            setupPostgreSQLIncrementFromWAL(snapshot, startWAL);
            break;
        default:
            // 对于定时同步类型，使用记录的时间戳
            String startTime = snapshot.get("incrementStartTime");
            setupTimingIncrementFromTime(snapshot, startTime);
    }
    
    profileComponent.editConfigModel(meta);
}
```

#### 4.5.3 时序图：确保数据一致性的关键步骤

```
sequenceDiagram
    participant App as FullIncrementPuller
    participant DB as 源数据库
    participant Meta as Meta存储
    participant Full as FullPuller
    participant Inc as IncrementPuller
    
    Note over App,Inc: 关键：先记录位置，再全量同步
    
    App->>DB: 1. 查询当前偏移量位置
    DB-->>App: 返回binlog/SCN/LSN等位置
    App->>Meta: 2. 保存起始位置到snapshot
    
    Note over App,Full: 全量同步阶段
    App->>Full: 3. 开始全量同步
    Note over DB: 业务系统持续写入新数据
    Full->>DB: 4. 分批读取历史数据
    Full->>Meta: 5. 更新全量同步进度
    
    Note over App,Inc: 增量同步阶段
    App->>Inc: 6. 全量完成后启动增量
    App->>Meta: 7. 设置增量从记录位置开始
    Inc->>DB: 8. 从起始位置开始监听变更
    Note over Inc: 能够捕获全量期间的所有变更
```

### 4.6 异常处理机制（纯异常驱动）

#### 4.6.1 异常恢复策略

| 异常类型 | 处理方式 | 恢复机制 |
|---------|---------|----------|
| 连接异常 | 发布ClosedEvent | 用户手动重启或系统重启自动恢复 |
| 数据异常 | 发布ClosedEvent | 从FULL_PENDING重新开始 |
| 系统异常 | 发布ClosedEvent | ManagerFactory自动重置为READY状态 |

**优势**：
- ✅ **零额外状态**：完全复用现有的状态管理机制
- ✅ **零性能开销**：异常时才触发处理，正常情况下无额外检查
- ✅ **简洁高效**：利用现有的事件驱动机制

## 5. UI界面改造

### 5.1 同步模式选择扩展
- 在驱动配置页面的同步模式下拉框中新增"全量+增量"选项
- 新增混合模式的配置参数界面

### 5.2 监控界面增强
```
<!-- 混合模式状态显示 -->
<div th:if="${mapping.model eq 'fullIncrement'}">
    <div class="sync-phase-indicator">
        <span class="phase-label" th:classappend="${meta.fullIncrementState eq 'FULL_RUNNING'} ? 'active' : ''">
            全量同步阶段
        </span>
        <span class="phase-separator">→</span>
        <span class="phase-label" th:classappend="${meta.fullIncrementState eq 'INCREMENT_RUNNING'} ? 'active' : ''">
            增量同步阶段
        </span>
    </div>
    
    <!-- 详细进度信息 -->
    <div class="phase-details">
        <div th:if="${meta.fullIncrementState eq 'FULL_RUNNING'}">
            全量进度: <span th:text="${meta.fullProgress}">0%</span>
            预计剩余时间: <span th:text="${meta.estimatedTime}">--</span>
        </div>
        <div th:if="${meta.fullIncrementState eq 'INCREMENT_RUNNING'}">
            增量状态: 实时同步中
            处理速度: <span th:text="${meta.incrementTps}">0</span> TPS
        </div>
    </div>
</div>
```

### 5.3 配置参数扩展
```
// 混合模式配置项
var fullIncrementConfig = {
    fullSyncTimeout: 7200,        // 全量同步超时时间（秒）
    autoRetryOnFailure: true,     // 失败自动重试
    maxRetryAttempts: 3,          // 最大重试次数
    consistencyCheckEnabled: true, // 一致性检查开关
    incrementStartDelay: 60       // 增量启动延迟（秒）
};
```

## 6. 配置和参数设计

### 6.1 新增配置参数
```
// 在Mapping类中新增混合模式配置
public class FullIncrementConfig {
    private int fullSyncTimeoutSeconds = 7200;      // 全量超时时间
    private boolean autoStartIncrement = true;      // 自动启动增量
    private int incrementStartDelaySeconds = 60;    // 增量启动延迟
    private boolean enableConsistencyCheck = true;  // 启用一致性检查
    private int healthCheckIntervalSeconds = 30;    // 健康检查间隔
    private int maxRetryAttempts = 3;               // 最大重试次数
    private boolean allowManualSwitch = false;      // 允许手动切换
}
```

### 6.2 配置验证
```
public class FullIncrementConfigValidator {
    
    public void validate(FullIncrementConfig config, Mapping mapping) {
        // 验证配置合理性
        if (config.getFullSyncTimeoutSeconds() < 300) {
            throw new BizException("全量同步超时时间不能少于5分钟");
        }
        
        if (config.getIncrementStartDelaySeconds() < 0) {
            throw new BizException("增量启动延迟不能为负数");
        }
        
        // 验证数据源是否支持增量同步
        validateIncrementSupport(mapping);
    }
    
    private void validateIncrementSupport(Mapping mapping) {
        ConnectorConfig sourceConfig = getSourceConnectorConfig(mapping);
        String connectorType = sourceConfig.getConnectorType();
        
        if (!SUPPORTED_INCREMENT_TYPES.contains(connectorType.toLowerCase())) {
            throw new BizException(String.format("数据源类型 %s 不支持增量同步", connectorType));
        }
    }
}
```

## 7. 性能考虑和优化

### 7.1 资源管理优化
```
public class MixedSyncResourceManager {
    
    // 为混合模式分配独立的线程池
    private final ExecutorService fullSyncExecutor = 
        Executors.newCachedThreadPool(new NamedThreadFactory("full-sync-"));
    
    private final ExecutorService incrementSyncExecutor = 
        Executors.newCachedThreadPool(new NamedThreadFactory("increment-sync-"));
    
    // 智能资源分配
    public ExecutorService allocateExecutor(SyncPhase phase, Mapping mapping) {
        switch (phase) {
            case FULL:
                // 全量阶段使用更多线程资源
                return fullSyncExecutor;
            case INCREMENT:
                // 增量阶段使用轻量级线程池
                return incrementSyncExecutor;
            default:
                return ForkJoinPool.commonPool();
        }
    }
}
```

### 7.2 内存使用优化
```
// 在全量到增量切换时清理不必要的缓存
private void optimizeMemoryUsage(String metaId) {
    // 清理全量同步相关的缓存
    clearFullSyncCache(metaId);
    
    // 为增量同步预热必要的缓存
    preheatIncrementCache(metaId);
    
    // 建议JVM进行垃圾回收
    System.gc();
}
```

## 8. 测试策略

### 8.1 单元测试
- `FullIncrementPuller` 核心逻辑测试
- 状态转换测试
- 异常处理测试
- 配置验证测试

### 8.2 集成测试
- 全量到增量的完整流程测试
- 不同数据源类型的兼容性测试
- 大数据量场景下的性能测试
- 网络异常和恢复测试

### 8.3 压力测试
- 长时间运行稳定性测试
- 高并发场景下的资源使用测试
- 内存泄漏检测

## 9. 风险评估与缓解措施

### 9.1 技术风险

| 风险项 | 风险等级 | 影响 | 缓解措施 |
|--------|----------|------|----------|
| 全量到增量切换时数据丢失 | 高 | 数据不一致 | 实现严格的一致性检查机制 |
| 混合模式状态管理复杂 | 中 | 系统稳定性 | 详细的状态机设计和测试 |
| 资源占用过高 | 中 | 系统性能 | 智能资源分配和监控 |
| 异常恢复机制不完善 | 中 | 服务可用性 | 多层次的错误处理和重试机制 |

### 9.2 兼容性风险
- 现有全量/增量模式的向后兼容性保证
- 不同数据库类型对混合模式的支持程度验证

## 10. 开发计划和里程碑

### 10.1 开发阶段划分

#### 第一阶段：基础架构（预计 2 周）
- [ ] `ModelEnum` 扩展实现
- [ ] `FullIncrementPuller` 基础框架
- [ ] `ManagerFactory` 改造
- [ ] 基础状态管理机制

#### 第二阶段：核心功能（预计 2 周）
- [ ] 全量到增量切换逻辑
- [ ] 数据一致性保证机制
- [ ] 异常处理和恢复机制
- [ ] 配置参数和验证

#### 第三阶段：UI和监控（预计 1 周）
- [ ] 界面改造和配置选项
- [ ] 混合模式监控面板
- [ ] 状态展示和操作控制

#### 第四阶段：测试和优化（预计 1 周）
- [ ] 单元测试和集成测试
- [ ] 性能测试和优化
- [ ] 文档完善

### 10.2 关键里程碑

| 里程碑 | 时间点 | 交付内容 |
|--------|--------|----------|
| M1 | 第2周末 | 完成基础架构，支持混合模式创建和基本切换 |
| M2 | 第4周末 | 完成核心功能，支持完整的全量+增量流程 |
| M3 | 第5周末 | 完成UI改造，支持混合模式配置和监控 |
| M4 | 第6周末 | 完成测试验证，达到生产环境部署标准 |

### 10.3 工作流程图

**关键改进**：在全量同步前记录增量同步起始位置是确保数据一致性的核心！

```
flowchart TD
    A[开始] --> B[初始化混合模式状态]
    B --> C["🔑 记录增量同步起始位置"]
    C --> D["执行全量同步"]
    D --> E{"全量同步完成?"}
    E --> |否| D
    E --> |是| F["切换到增量模式"]
    F --> G["从记录位置启动增量监听"]
    G --> H["捕获数据变更事件"]
    H --> I["处理变更事件"]
    I --> J["更新偏移量"]
    J --> H
    
    style C fill:#ff9999,stroke:#ff0000,stroke-width:3px
    style C color:#ffffff
    
    classDef keyStep fill:#ffeb3b,stroke:#f57f17,stroke-width:2px
    class C keyStep
```

**说明**：
- 🔑 **步骤3是关键**：在开始全量同步前，必须先记录当前的增量位置（binlog/SCN/LSN等）
- 这样确保增量同步能够捕获全量同步期间的所有数据变更，避免数据丢失
- 不同数据库使用不同的位置标识：MySQL用binlog位置，Oracle用SCN，SQL Server用LSN等

## 11. 总结

实现全量+增量混合模式需要对DBSyncer的核心架构进行重要改造，主要工作包括：

1. **架构扩展**：新增`FULL_INCREMENT`模式和`FullIncrementPuller`实现类
2. **状态管理**：增强`Meta`类支持混合模式状态跟踪
3. **协调机制**：实现全量和增量任务的启停协调
4. **一致性保证**：确保模式切换时的数据完整性
5. **异常处理**：完善的错误恢复和重试机制
6. **UI改造**：支持混合模式的配置和监控界面

预计总开发工作量约为 **42小时**（与计划文档中的评估一致），需要6周时间完成。该功能的实现将显著提升DBSyncer的使用场景覆盖范围和用户体验。