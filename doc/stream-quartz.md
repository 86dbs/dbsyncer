# 基于全量模式的定时增量同步设计

## 设计理念

### 核心思想
**每次定时增量 = 一次独立的全量同步**，通过动态创建和销毁独立的 mapping 来保证数据完整性，避免传统增量同步的数据丢失问题。

**关键理解：**
- 每次定时触发都构建一个独立的 mapping
- 通过时间窗口过滤实现增量效果
- 使用全量模式保证数据完整性

### 技术优势
- **数据完整性**：基于全量模式，数据不会丢失
- **断点续传**：复用全量模式的游标机制，支持断点续传
- **资源管理**：动态创建销毁任务，避免长期占用资源
- **技术成熟**：复用现有的全量同步机制，维护成本低

## 架构设计

### 组件架构
```
AbstractQuartzListener (现有定时机制)
    ↓
TimedFullSyncListener (定时全量监听器)
    ↓
FullPuller (现有全量同步器)
    ↓
ParserComponentImpl (现有数据处理器)
    ↓
Target Database (目标数据库)
```

### 数据流程
```
定时触发 → 创建独立Mapping → 设置时间窗口过滤 → 执行全量同步 → 更新同步时间 → 销毁独立Mapping
```

**流程说明：**
- **每次定时触发**：创建一个完全独立的 mapping
- **时间窗口过滤**：通过 SQL 条件实现增量效果
- **全量同步执行**：使用现有的全量同步机制
- **资源清理**：执行完成后销毁临时资源

## 技术实现

### 1. 基于现有触发机制
```java
// 使用现有的 AbstractQuartzListener 机制
public class TimedFullSyncListener extends AbstractQuartzListener {
    
    @Override
    protected Point checkLastPoint(Map<String, String> command, int index) {
        // 设置时间窗口过滤条件
        String beginTime = getLastSyncTime();
        String endTime = getCurrentTime();
        
        // 在命令中添加时间过滤条件
        command.put("timeFilter", "create_time >= '" + beginTime + "' AND create_time < '" + endTime + "'");
        
        return new Point(command, null);
    }
    
    @Override
    public void run() {
        // 每次定时触发都创建一个独立的 mapping
        createIndependentMapping();
        
        // 复用现有的定时任务执行逻辑
        super.run();
    }
    
    private void createIndependentMapping() {
        // 创建独立的 mapping 用于本次增量同步
        // 设置时间窗口过滤条件
        // 启动全量同步
    }
}
```

### 2. 独立Mapping管理
```java
// 动态创建和销毁独立的Mapping
@Component
public class TimedFullSyncMappingManager {
    
    @Resource
    private ProfileComponent profileComponent;
    
    /**
     * 创建独立的Mapping
     */
    public TimedFullSyncResources createIndependentMapping(Mapping parentMapping) {
        // 1. 创建独立的Meta
        Meta independentMeta = createIndependentMeta(parentMapping);
        
        // 2. 创建独立的Mapping
        Mapping independentMapping = createIndependentMapping(parentMapping, independentMeta);
        
        // 3. 创建独立的TableGroup
        List<TableGroup> independentTableGroups = createIndependentTableGroups(parentMapping, independentMapping);
        
        return new TimedFullSyncResources(independentMeta, independentMapping, independentTableGroups);
    }
    
    /**
     * 销毁独立资源
     */
    public void destroyIndependentResources(TimedFullSyncResources resources) {
        // 1. 销毁独立TableGroup
        resources.getIndependentTableGroups().forEach(tableGroup -> {
            profileComponent.removeTableGroup(tableGroup.getId());
        });
        
        // 2. 销毁独立Mapping
        profileComponent.removeConfigModel(resources.getIndependentMapping().getId());
        
        // 3. 销毁独立Meta
        profileComponent.removeConfigModel(resources.getIndependentMeta().getId());
        
        // 4. 更新父Meta的最后同步时间
        updateParentLastSyncTime(resources.getIndependentMeta());
    }
    
    private Meta createIndependentMeta(Mapping parentMapping) {
        Meta independentMeta = new Meta(profileComponent);
        independentMeta.setId(UUIDUtil.getUUID());
        independentMeta.setMappingId(parentMapping.getId());
        independentMeta.setParentMetaId(parentMapping.getMetaId());
        
        // 设置时间窗口过滤条件
        setTimeWindowFilter(independentMeta, parentMapping);
        
        profileComponent.addConfigModel(independentMeta);
        return independentMeta;
    }
    
    private Mapping createIndependentMapping(Mapping parentMapping, Meta independentMeta) {
        Mapping independentMapping = parentMapping.clone();
        independentMapping.setId(UUIDUtil.getUUID());
        independentMapping.setMetaId(independentMeta.getId());
        independentMapping.setName(parentMapping.getName() + "_independent_" + System.currentTimeMillis());
        
        // 设置时间窗口过滤条件到Mapping参数中
        setTimeWindowFilterToMapping(independentMapping);
        
        profileComponent.addConfigModel(independentMapping);
        return independentMapping;
    }
    
    private List<TableGroup> createIndependentTableGroups(Mapping parentMapping, Mapping independentMapping) {
        List<TableGroup> parentTableGroups = profileComponent.getTableGroupAll(parentMapping.getId());
        List<TableGroup> independentTableGroups = new ArrayList<>();
        
        for (TableGroup parentTableGroup : parentTableGroups) {
            TableGroup independentTableGroup = parentTableGroup.clone();
            independentTableGroup.setId(UUIDUtil.getUUID());
            independentTableGroup.setMappingId(independentMapping.getId());
            
            // 设置时间窗口过滤条件到TableGroup命令中
            setTimeWindowFilterToTableGroup(independentTableGroup);
            
            profileComponent.addTableGroup(independentTableGroup);
            independentTableGroups.add(independentTableGroup);
        }
        
        return independentTableGroups;
    }
}
```

### 3. 集成现有全量同步
```java
// 在 FullPuller 中集成定时全量同步
@Component
public class TimedFullSyncPuller extends FullPuller {
    
    @Resource
    private TimedFullSyncMappingManager mappingManager;
    
    @Override
    public void start(Mapping mapping) {
        // 创建独立的Mapping
        TimedFullSyncResources resources = mappingManager.createIndependentMapping(mapping);
        
        try {
            // 调用父类的全量同步逻辑
            super.start(resources.getIndependentMapping());
        } finally {
            // 销毁独立资源
            mappingManager.destroyIndependentResources(resources);
        }
    }
}

// 独立资源封装类
public class TimedFullSyncResources {
    private final Meta independentMeta;
    private final Mapping independentMapping;
    private final List<TableGroup> independentTableGroups;
    
    public TimedFullSyncResources(Meta independentMeta, Mapping independentMapping, List<TableGroup> independentTableGroups) {
        this.independentMeta = independentMeta;
        this.independentMapping = independentMapping;
        this.independentTableGroups = independentTableGroups;
    }
    
    // getters...
}
```

## 配置说明

### 增量条件配置
```java
public class TimedFullSyncConfig {
    private String cron;           // 调度表达式
    private String timeField;     // 时间字段名
    private String filterType;    // 过滤类型：timestamp/date
    private int batchSize;        // 批次大小
    private boolean enableCursor; // 是否启用游标
}
```

### 过滤条件生成
```java
public class IncrementFilterBuilder {
    
    public Map<String, String> buildFilter(TimedFullSyncConfig config) {
        Map<String, String> filter = new HashMap<>();
        
        if ("timestamp".equals(config.getFilterType())) {
            filter.put("beginTime", getLastTimestamp());
            filter.put("endTime", getCurrentTimestamp());
        } else if ("date".equals(config.getFilterType())) {
            filter.put("beginDate", getLastDate());
            filter.put("endDate", getCurrentDate());
        }
        
        return filter;
    }
}
```

## 实施指导

### 1. 实现步骤
1. **扩展现有监听器**：继承 `AbstractQuartzListener` 实现 `TimedFullSyncListener`
2. **实现独立Mapping管理**：创建 `TimedFullSyncMappingManager` 管理独立Mapping的创建和销毁
3. **集成现有全量同步**：在 `FullPuller` 中集成定时全量同步逻辑
4. **建立父子关系**：在Meta中建立父子关系，支持增量时间窗口管理

### 2. 关键实现点
- **独立Mapping管理**：每次定时触发时动态创建独立的Mapping（Meta、Mapping、TableGroup），执行完成后销毁
- **时间窗口过滤**：通过SQL条件实现增量效果，每次增量都有独立的时间窗口
- **全量同步复用**：直接使用 `FullPuller` 的全量同步机制
- **数据去重**：基于主键去重，避免重复数据

### 3. 注意事项
- **独立Mapping生命周期**：确保独立Mapping的创建和销毁不会影响父Mapping
- **时间窗口管理**：合理设置增量时间窗口，避免数据丢失或重复
- **性能优化**：大数据量时需要分批处理
- **监控告警**：需要完善的监控机制，包括独立Mapping的状态监控

## 优势对比

| 维度 | 传统定时增量 | 基于全量的定时增量 |
|------|-------------|------------------|
| **数据完整性** | 可能丢失数据 | 数据完整，不丢失 |
| **断点续传** | 基于时间戳 | 基于游标，更精确 |
| **故障恢复** | 时间窗口问题 | 全量模式，恢复完整 |
| **资源管理** | 长期占用 | 动态创建销毁 |
| **性能** | 时间窗口限制 | 全量模式，性能更好 |

## 总结

基于全量模式的定时增量同步设计，**完全复用现有的触发机制**，既解决了数据丢失问题，又保持了技术架构的简洁性。通过**动态创建和销毁独立的Mapping**（Meta、Mapping、TableGroup），建立了完整的资源生命周期管理，实现了资源的高效利用，同时保证了数据的完整性和一致性。

**核心优势：**
- **独立Mapping管理**：每次定时触发时动态创建独立的Mapping，执行完成后销毁
- **时间窗口过滤**：通过SQL条件实现增量效果，每次增量都有独立的时间窗口
- **技术成熟**：复用已验证的全量同步机制
- **维护简单**：基于现有代码，维护成本低

**关键理解：**
- **每次定时增量 = 一次独立的全量同步**
- **每次增量都需要构建一个独立的 mapping**
- **通过时间窗口过滤实现增量效果**