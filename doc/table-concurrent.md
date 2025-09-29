# 流式处理中TableGroup并发Cursor管理方案

## 问题描述

在DBSyncer的流式处理中，一个Task包含多个TableGroup，这会同时启动多个流式处理。但当前的Meta.snapshot中的cursor是共享的，无法区分是哪个TableGroup的cursor，导致以下问题：

1. **Cursor冲突**：多个TableGroup同时更新同一个cursor，相互覆盖
2. **断点续传失效**：无法准确恢复每个TableGroup的处理进度
3. **数据丢失风险**：cursor被覆盖可能导致数据重复处理或遗漏

## 解决方案

### 基于TableGroup自身状态管理的简化方案

#### 1.1 核心思路
既然TableGroup自身是可以序列化的，为什么不把cursor直接从Meta.snapshot中移除，让每个TableGroup维护自己的cursor状态？这样既避免了冲突，又简化了实现。

#### 1.2 实现方案

##### 1.2.1 扩展TableGroup模型
```java
public class TableGroup extends AbstractConfigModel {
    // 现有字段...
    private int index; // 已有的排序索引，可以作为tableGroupIndex使用
    
    // 新增字段 - 流式处理状态管理
    private Object[] cursors; // 当前TableGroup的cursor
    private boolean streamingCompleted; // 流式处理是否完成
    private String errorMessage; // 错误信息
    
    // getter/setter方法
    public Object[] getCursors() {
        return cursors;
    }
    
    public void setCursors(Object[] cursors) {
        this.cursors = cursors;
    }
    
    public boolean isStreamingCompleted() {
        return streamingCompleted;
    }
    
    public void setStreamingCompleted(boolean streamingCompleted) {
        this.streamingCompleted = streamingCompleted;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
    
    public boolean HasError() {
        return Strings.;
    }
    
    /**
     * 记录错误信息
     */
    public void recordError(String errorMessage) {
        this.errorMessage = errorMessage;
        this.streamingCompleted = false;
    }
    
    /**
     * 清理错误状态
     */
    public void clearError() {
        this.hasError = false;
        this.errorMessage = null;
    }
    
    /**
     * 检查是否有错误
     */
    public boolean hasError() {
        return hasError;
    }
}
```


##### 1.2.2 修改FullPuller.doTask()方法
```java
private void doTask(Task task, Mapping mapping, List<TableGroup> list, Executor executor) {
    // 记录开始时间
    long now = Instant.now().toEpochMilli();
    task.setBeginTime(now);
    task.setEndTime(now);

    // 获取上次同步点 - 不再从Meta.snapshot获取cursor
    Meta meta = profileComponent.getMeta(task.getId());
    
    // 启动时清理所有TableGroup的错误状态
    for (TableGroup tableGroup : list) {
        tableGroup.clearError();
        profileComponent.editConfigModel(tableGroup);
    }
    
    // 并发处理所有TableGroup
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (int i = 0; i < list.size(); i++) {
        final TableGroup tableGroup = list.get(i);
        
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            try {
                // 直接使用TableGroup的cursor进行流式处理
                parserComponent.executeTableGroup(tableGroup, mapping, executor);
                
                // 处理完成后标记完成状态
                tableGroup.setStreamingCompleted(true);
                
                // 保存TableGroup状态
                profileComponent.editConfigModel(tableGroup);
                
            } catch (Exception e) {
                logger.error("TableGroup {} 处理失败", tableGroup.getIndex(), e);
                
                // 记录TableGroup的错误状态
                tableGroup.recordError(e.getMessage());
                profileComponent.editConfigModel(tableGroup);
                
                // 记录系统级错误
                meta.saveState(MetaEnum.ERROR, e.getMessage());
                logService.log(LogType.SystemLog.ERROR, e.getMessage());
            }
        }, executor);
        
        futures.add(future);
    }
    
    // 等待所有TableGroup完成
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    
    // 记录结束时间
    task.setEndTime(Instant.now().toEpochMilli());
    flush(task);

    // 检查并执行 Meta 中的阶段处理方法
    Runnable phaseHandler = meta.getPhaseHandler();
    if (phaseHandler != null) {
        phaseHandler.run();
    } else {
        meta.resetState();
    }
}
```

##### 1.2.3 简化flush方法
```java
private void flush(Task task) {
    Meta meta = profileComponent.getMeta(task.getId());
    Assert.notNull(meta, "检查meta为空.");

    // 全量的过程中，有新数据则更新总数
    long finished = meta.getSuccess().get() + meta.getFail().get();
    if (meta.getTotal().get() < finished) {
        meta.getTotal().set(finished);
    }

    meta.setBeginTime(task.getBeginTime());
    meta.setEndTime(task.getEndTime());
    meta.setUpdateTime(Instant.now().toEpochMilli());
    
    // 不再需要保存cursor到Meta.snapshot
    // cursor现在由各个TableGroup自己管理
    
    profileComponent.editConfigModel(meta);
}
```

##### 1.2.4 实现executeTableGroup方法
```java
@Override
public void executeTableGroup(TableGroup tableGroup, Mapping mapping, Executor executor) {
    try {
        // 创建TableGroup专用的上下文
        AbstractPluginContext context = createTableGroupContext(tableGroup, mapping);

        // 获取数据库连接器
        Connector sourceConnector = profileComponent.getConnector(mapping.getSourceConnectorId());
        Database db = (Database) connectorFactory.getConnectorService(sourceConnector.getType());
        DatabaseConnectorInstance sourceConnectorInstance = (DatabaseConnectorInstance) connectorFactory.connect(sourceConnector.getConfig());
        context.setSourceConnectorInstance(sourceConnectorInstance);

        // 获取主键列表
        List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(tableGroup.getSourceTable());

        // 根据是否支持cursor选择不同的SQL
        boolean supportedCursor = null != tableGroup.getCursors();
        String querySql;
        if (supportedCursor) {
            querySql = context.getCommand().get(ConnectorConstant.OPERTION_QUERY_CURSOR);
        } else {
            querySql = context.getCommand().get(ConnectorConstant.OPERTION_QUERY_STREAM);
        }

        // 执行流式处理
        ((DatabaseConnectorInstance) context.getSourceConnectorInstance()).execute(databaseTemplate ->
                executeTableGroupWithStreaming(tableGroup, context, db, executor, primaryKeys, querySql, databaseTemplate));

    } catch (Exception e) {
        logger.error("TableGroup {} 流式处理失败", tableGroup.getIndex(), e);
        
        // 记录TableGroup的错误状态
        tableGroup.setErrorMessage(e.getMessage());
        profileComponent.editConfigModel(tableGroup);
        
        throw e;
    }
}
```

##### 1.2.5 修改流式处理逻辑
```java
// 新的TableGroup流式处理方法
private void executeTableGroupWithStreaming(TableGroup tableGroup, AbstractPluginContext pluginContext, 
                                          Database db, Executor executor, List<String> primaryKeys, 
                                          String metaId, String querySql, 
                                          DatabaseTemplate databaseTemplate) {
    // 获取流式处理的fetchSize
    Integer fetchSize = db.getStreamingFetchSize(pluginContext);
    databaseTemplate.setFetchSize(fetchSize);

    try (Stream<Map<String, Object>> stream = databaseTemplate.queryForStream(querySql,
            new ArgumentPreparedStatementSetter(tableGroup.getCursors()),
            new ColumnMapRowMapper())) {
        List<Map<String, Object>> batch = new ArrayList<>();
        Iterator<Map<String, Object>> iterator = stream.iterator();

        while (iterator.hasNext()) {
            batch.add(iterator.next());

            // 达到批次大小时处理数据
            if (batch.size() >= pluginContext.getBatchSize()) {
                processTableGroupDataBatch(batch, tableGroup, pluginContext, executor, primaryKeys);
                batch = new ArrayList<>();
            }
        }

        // 处理最后一批数据
        if (!batch.isEmpty()) {
            processTableGroupDataBatch(batch, tableGroup, pluginContext, executor, primaryKeys);
            
            // 标记流式处理完成
            tableGroup.setStreamingCompleted(true);
            profileComponent.editConfigModel(tableGroup);
        }
    }
}
```

## 方案优势

### 1. 架构清晰
- **移除Meta.snapshot中的cursor管理**：不再需要复杂的命名空间隔离
- **TableGroup自管理**：每个TableGroup维护自己的cursor状态
- **Task不再关注具体TableGroup**：Task只负责协调，不管理具体状态
- **流式处理无需分页**：正如您所说，流式处理不需要pageIndex

### 2. 解决并发问题
- **完全隔离**：每个TableGroup的cursor完全独立，无冲突
- **真正并发**：多个TableGroup可以真正并发处理
- **精确断点续传**：每个TableGroup可以独立恢复处理进度
- **使用TableGroup.index**：解决tableGroupIndex传递问题

### 3. 实现简洁
- **最小改动**：只需要在TableGroup中添加cursor字段
- **职责明确**：Task负责协调，TableGroup负责自身状态管理
- **向后兼容**：不影响现有的分页处理逻辑
- **易于维护**：状态管理逻辑清晰，职责明确

### 4. 错误处理机制
- **独立错误记录**：每个TableGroup可以记录自己的错误状态
- **启动时清理错误**：每次启动时自动清理所有TableGroup的错误状态
- **错误隔离**：某个TableGroup的错误不会影响其他TableGroup的处理
- **错误恢复**：可以针对特定TableGroup进行错误恢复和重试

## 实施步骤

1. **扩展TableGroup模型**：添加cursor、streamingCompleted和错误处理字段
2. **修改FullPuller**：移除Meta.snapshot中的cursor管理逻辑，添加错误处理
3. **更新流式处理**：在流式处理过程中直接更新TableGroup的cursor
4. **添加错误处理**：实现TableGroup级别的错误记录和清理机制
5. **测试验证**：重点测试并发场景、断点续传功能和错误处理

## 注意事项

1. **序列化兼容性**：确保TableGroup的序列化/反序列化正常工作
2. **状态一致性**：确保TableGroup状态更新的原子性
3. **错误处理**：某个TableGroup失败不应影响其他TableGroup的状态
4. **性能考虑**：频繁更新TableGroup状态可能影响性能，需要合理控制更新频率
5. **持久化时机**：每次cursor更新后立即调用`profileComponent.editConfigModel(tableGroup)`进行持久化
6. **断点续传**：系统重启后，TableGroup可以从持久化的cursor状态恢复处理
7. **错误清理**：每次启动时自动清理所有TableGroup的错误状态
8. **错误隔离**：TableGroup级别的错误记录，避免全局错误影响
9. **错误恢复**：可以针对特定TableGroup进行错误恢复和重试
