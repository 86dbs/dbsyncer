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

### 2.2 当前架构限制

1. **模式互斥性**：`ModelEnum`仅支持`FULL`和`INCREMENT`两种独立模式
2. **组件隔离**：`FullPuller`和`IncrementPuller`完全独立，无协调机制
3. **状态管理**：`Meta`状态无法同时跟踪全量和增量进度
4. **调度机制**：`ManagerFactory`按单一模式获取`Puller`，缺乏混合模式调度能力

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
    
    // 混合模式状态枚举
    enum SyncState {
        FULL_PENDING,     // 全量待执行
        FULL_RUNNING,     // 全量执行中
        FULL_COMPLETED,   // 全量完成
        INCREMENT_STARTING, // 增量启动中
        INCREMENT_RUNNING, // 增量执行中
        ERROR_RECOVERY    // 错误恢复中
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
            // 1. 初始化混合模式状态
            initFullIncrementState(mapping);
            
            // 2. 执行全量同步
            executeFullSync(mapping);
            
            // 3. 等待全量完成
            waitForFullCompletion(mapping);
            
            // 4. 启动增量同步
            startIncrementSync(mapping);
            
        } catch (Exception e) {
            handleError(mapping, e);
        }
    });
    
    coordinator.setName("full-increment-coordinator-" + mapping.getId());
    coordinator.start();
}

private void executeFullSync(Mapping mapping) {
    syncStates.put(mapping.getMetaId(), SyncState.FULL_RUNNING);
    
    // 创建全量同步的临时Mapping，确保模式为FULL
    Mapping fullMapping = cloneMapping(mapping);
    fullMapping.setModel(ModelEnum.FULL.getCode());
    
    fullPuller.start(fullMapping);
}

private void waitForFullCompletion(Mapping mapping) {
    String metaId = mapping.getMetaId();
    
    while (true) {
        Meta meta = profileComponent.getMeta(metaId);
        if (isFullSyncCompleted(meta)) {
            syncStates.put(metaId, SyncState.FULL_COMPLETED);
            break;
        }
        
        Thread.sleep(1000); // 检查间隔
    }
}

private void startIncrementSync(Mapping mapping) {
    syncStates.put(mapping.getMetaId(), SyncState.INCREMENT_STARTING);
    
    // 重置Meta状态，准备增量同步
    resetMetaForIncrement(mapping);
    
    // 创建增量同步的临时Mapping
    Mapping incrementMapping = cloneMapping(mapping);
    incrementMapping.setModel(ModelEnum.INCREMENT.getCode());
    
    incrementPuller.start(incrementMapping);
    syncStates.put(mapping.getMetaId(), SyncState.INCREMENT_RUNNING);
}
```

### 4.3 状态管理增强

#### 4.3.1 Meta 扩展设计
在现有`Meta`类的`snapshot`中新增混合模式专用字段：

```java
// 在Meta.snapshot中新增的字段
public static final String FULL_INCREMENT_STATE = "fullIncrementState";
public static final String FULL_START_TIME = "fullStartTime";
public static final String FULL_END_TIME = "fullEndTime";
public static final String INCREMENT_START_TIME = "incrementStartTime";
public static final String LAST_SYNC_CHECKPOINT = "lastSyncCheckpoint";
```

#### 4.3.2 状态持久化
```java
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
```java
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

#### 4.5.1 切换点同步机制
```java
private void ensureDataConsistency(Mapping mapping) {
    String metaId = mapping.getMetaId();
    
    // 1. 记录全量同步完成时的时间点
    long fullSyncEndTime = System.currentTimeMillis();
    
    // 2. 根据数据源类型设置增量同步起始点
    setupIncrementStartPoint(mapping, fullSyncEndTime);
    
    // 3. 确保增量同步能捕获全量同步期间的变更
    validateConsistencyCheckpoint(mapping);
}

private void setupIncrementStartPoint(Mapping mapping, long fullSyncEndTime) {
    Meta meta = profileComponent.getMeta(mapping.getMetaId());
    Map<String, String> snapshot = meta.getSnapshot();
    
    // 根据不同数据源类型设置合适的起始偏移量
    ConnectorConfig sourceConfig = getSourceConnectorConfig(mapping);
    String connectorType = sourceConfig.getConnectorType();
    
    switch (connectorType.toLowerCase()) {
        case "mysql":
            // 设置 binlog 位置为全量同步开始前的位置
            setupMySQLIncrementPoint(snapshot, fullSyncEndTime);
            break;
        case "oracle":
            // 设置 SCN 为全量同步开始前的位置
            setupOracleIncrementPoint(snapshot, fullSyncEndTime);
            break;
        case "sqlserver":
            // 设置 LSN 为全量同步开始前的位置
            setupSQLServerIncrementPoint(snapshot, fullSyncEndTime);
            break;
        default:
            // 对于定时同步类型，设置合适的时间戳
            setupTimingIncrementPoint(snapshot, fullSyncEndTime);
    }
    
    profileComponent.editConfigModel(meta);
}
```

### 4.6 异常处理和恢复机制

#### 4.6.1 故障检测
```java
private void monitorSyncHealth(Mapping mapping) {
    String metaId = mapping.getMetaId();
    
    ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
    monitor.scheduleAtFixedRate(() -> {
        try {
            SyncState currentState = syncStates.get(metaId);
            Meta meta = profileComponent.getMeta(metaId);
            
            // 检查全量同步是否异常超时
            if (currentState == SyncState.FULL_RUNNING) {
                checkFullSyncTimeout(mapping, meta);
            }
            
            // 检查增量同步是否异常中断
            if (currentState == SyncState.INCREMENT_RUNNING) {
                checkIncrementSyncHealth(mapping, meta);
            }
            
        } catch (Exception e) {
            logger.error("同步健康检查异常: " + metaId, e);
        }
    }, 30, 30, TimeUnit.SECONDS);
}

private void handleSyncFailure(Mapping mapping, Exception error) {
    String metaId = mapping.getMetaId();
    SyncState currentState = syncStates.get(metaId);
    
    logger.error("混合同步异常: metaId={}, state={}, error={}", 
                 metaId, currentState, error.getMessage(), error);
    
    // 根据当前状态采取不同的恢复策略
    switch (currentState) {
        case FULL_RUNNING:
            handleFullSyncFailure(mapping, error);
            break;
        case INCREMENT_RUNNING:
            handleIncrementSyncFailure(mapping, error);
            break;
        default:
            // 通用错误处理
            syncStates.put(metaId, SyncState.ERROR_RECOVERY);
            logService.log(LogType.SystemLog.ERROR, 
                          String.format("混合同步异常，进入错误恢复状态: %s", error.getMessage()));
    }
}
```

## 5. UI界面改造

### 5.1 同步模式选择扩展
- 在驱动配置页面的同步模式下拉框中新增"全量+增量"选项
- 新增混合模式的配置参数界面

### 5.2 监控界面增强
```html
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
```javascript
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
```java
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
```java
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
```java
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
```java
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

## 11. 总结

实现全量+增量混合模式需要对DBSyncer的核心架构进行重要改造，主要工作包括：

1. **架构扩展**：新增`FULL_INCREMENT`模式和`FullIncrementPuller`实现类
2. **状态管理**：增强`Meta`类支持混合模式状态跟踪
3. **协调机制**：实现全量和增量任务的启停协调
4. **一致性保证**：确保模式切换时的数据完整性
5. **异常处理**：完善的错误恢复和重试机制
6. **UI改造**：支持混合模式的配置和监控界面

预计总开发工作量约为 **42小时**（与计划文档中的评估一致），需要6周时间完成。该功能的实现将显著提升DBSyncer的使用场景覆盖范围和用户体验。