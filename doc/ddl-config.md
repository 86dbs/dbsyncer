# DDL 同步配置方案

## 一、概述

为了满足 DDL 同步的精细化控制需求，在现有 `enableDDL` 配置基础上，新增细粒度的 DDL 行为控制配置项。这些配置项用于约束和控制不同类型的 DDL 操作，提供更灵活的同步策略。

## 二、需求分析

### 2.1 现有配置限制

当前 DDL 同步仅通过 `ListenerConfig.enableDDL` 进行全局开关控制，无法细粒度控制：
- 无法区分不同类型的 DDL 操作（ADD、DROP、MODIFY、CHANGE）
- 字段映射机制在 DDL 同步时缺乏灵活的控制策略

### 2.2 新增配置需求

根据业务场景，需要支持以下配置项：

1. **允许加字段DDL**（`allowAddColumn`）
   - 缺省值：`true`
   - 用途：是否允许执行 `ALTER TABLE ... ADD COLUMN` 操作

3. **允许删字段DDL**（`allowDropColumn`）
   - 缺省值：`true`
   - 用途：是否允许执行 `ALTER TABLE ... DROP COLUMN` 操作

4. **允许变更字段DDL**（`allowModifyColumn`）
   - 缺省值：`true`
   - 用途：是否允许执行 `ALTER TABLE ... MODIFY/ALTER COLUMN` 操作（包括类型变更、长度变更、可空性变更等）

**注意**：字段重命名（`ALTER_CHANGE`）操作暂不单独配置，可归入 `allowModifyColumn` 控制。

## 三、技术方案设计

### 3.1 配置结构扩展

#### 3.1.1 ListenerConfig 扩展

在 `ListenerConfig` 类中新增 DDL 细粒度控制配置：

```java
package org.dbsyncer.sdk.config;

public class ListenerConfig {
    
    // ... 现有字段 ...
    
    /**
     * 禁用ddl事件
     */
    private boolean enableDDL;
    
    // ========== DDL 细粒度控制配置 ==========
    
    /**
     * 允许加字段DDL（ALTER_ADD）
     * 缺省值：true
     */
    private boolean allowAddColumn = true;
    
    /**
     * 允许删字段DDL（ALTER_DROP）
     * 缺省值：true
     */
    private boolean allowDropColumn = true;
    
    /**
     * 允许变更字段DDL（ALTER_MODIFY、ALTER_CHANGE）
     * 缺省值：true
     */
    private boolean allowModifyColumn = true;
    
    // ... getter/setter 方法 ...
}
```

#### 3.1.2 配置兼容性

- **向后兼容**：新增配置项均有缺省值，不影响现有配置
- **配置优先级**：`enableDDL = false` 时，所有 DDL 操作均被禁用，细粒度配置不生效
- **配置继承**：细粒度配置仅在 `enableDDL = true` 时生效

### 3.2 DDL 处理流程改造

#### 3.2.1 GeneralBufferActuator.parseDDl() 改造

在 `GeneralBufferActuator.parseDDl()` 方法中增加配置检查逻辑：

```java
public void parseDDl(WriterResponse response, Mapping mapping, TableGroup tableGroup) {
    try {
        ListenerConfig listenerConfig = mapping.getListener();
        
        // 1. 全局 DDL 开关检查
        if (!listenerConfig.isEnableDDL()) {
            logger.debug("DDL 同步已禁用，跳过 DDL 处理");
            return;
        }
        
        // 2. 解析 DDL 获取操作类型
        ConnectorConfig tConnConfig = getConnectorConfig(mapping.getTargetConnectorId());
        String tConnType = tConnConfig.getConnectorType();
        ConnectorService connectorService = connectorFactory.getConnectorService(tConnType);
        DDLConfig targetDDLConfig = ddlParser.parse(connectorService, tableGroup, response.getSql());
        
        // 3. 根据操作类型检查细粒度配置
        DDLOperationEnum operation = targetDDLConfig.getDdlOperationEnum();
        if (!isDDLOperationAllowed(listenerConfig, operation)) {
            logger.warn("DDL 操作被配置禁用，跳过执行。操作类型: {}, 表: {}", 
                operation, tableGroup.getTargetTable().getName());
            return;
        }
        
        // 4. 执行 DDL（原有逻辑）
        // 注意：表存在性检查已在保存表映射时完成，此处无需再次检查
        Result result = connectorFactory.writerDDL(tConnectorInstance, targetDDLConfig);
        // ... 后续处理 ...
    } catch (Exception e) {
        logger.error(e.getMessage(), e);
    }
}

/**
 * 检查 DDL 操作是否被允许
 */
private boolean isDDLOperationAllowed(ListenerConfig config, DDLOperationEnum operation) {
    switch (operation) {
        case ALTER_ADD:
            return config.isAllowAddColumn();
        case ALTER_DROP:
            return config.isAllowDropColumn();
        case ALTER_MODIFY:
        case ALTER_CHANGE:
            return config.isAllowModifyColumn();
        default:
            logger.warn("未知的 DDL 操作类型: {}", operation);
            return false;
    }
}

/**
 * 检查表是否存在
 */
private boolean checkTableExists(ConnectorInstance connectorInstance, String tableName) {
    try {
        // 通过查询表元信息判断表是否存在
        MetaInfo metaInfo = connectorFactory.getMetaInfo(connectorInstance, tableName);
        return metaInfo != null && metaInfo.getColumn() != null && !metaInfo.getColumn().isEmpty();
    } catch (Exception e) {
        // 表不存在时会抛出异常
        return false;
    }
}

/**
 * 自动创建目标表
 */
private void createTargetTable(Mapping mapping, TableGroup tableGroup, 
                               ConnectorInstance targetConnectorInstance) throws Exception {
    // 1. 获取源表结构
    ConnectorInstance sourceConnectorInstance = connectorFactory.connect(
        getConnectorConfig(mapping.getSourceConnectorId()));
    MetaInfo sourceMetaInfo = connectorFactory.getMetaInfo(
        sourceConnectorInstance, tableGroup.getSourceTable().getName());
    
    // 2. 生成 CREATE TABLE DDL
    ConnectorService targetConnectorService = connectorFactory.getConnectorService(
        getConnectorConfig(mapping.getTargetConnectorId()).getConnectorType());
    
    // 注意：这里需要 ConnectorService 提供生成 CREATE TABLE DDL 的方法
    // 如果 ConnectorService 中已有 generateCreateTableDDL 方法，直接调用
    // 否则需要扩展 ConnectorService 接口（参考本文档第 3.3.4 节）
    String createTableDDL = generateCreateTableDDL(
        targetConnectorService, sourceMetaInfo, tableGroup.getTargetTable().getName());
    
    // 3. 执行 CREATE TABLE DDL
    DDLConfig createTableConfig = new DDLConfig();
    createTableConfig.setSql(createTableDDL);
    createTableConfig.setDdlOperationEnum(DDLOperationEnum.CREATE_TABLE); // 需要新增此枚举值
    
    Result result = connectorFactory.writerDDL(targetConnectorInstance, createTableConfig);
    if (result.hasError()) {
        throw new RuntimeException("自动创建表失败: " + result.getError());
    }
    
    logger.info("自动创建表成功: {}", tableGroup.getTargetTable().getName());
}
```

#### 3.2.2 DDL 操作类型映射

DDL 操作类型与配置项的映射关系：

| DDLOperationEnum | 配置项 | 说明 |
|-----------------|--------|------|
| ALTER_ADD | allowAddColumn | 新增字段 |
| ALTER_DROP | allowDropColumn | 删除字段 |
| ALTER_MODIFY | allowModifyColumn | 修改字段属性（类型、长度等） |
| ALTER_CHANGE | allowModifyColumn | 重命名字段（归入修改类） |

**注意**：如果将来需要单独控制字段重命名，可以新增 `allowRenameColumn` 配置项。

### 3.3 表自动创建实现

#### 3.3.1 触发时机（推荐方案：配置时检查）

**核心思路**：在保存表映射（TableGroup）时检查目标表是否存在，如果不存在且允许创建，则提示用户是否创建。这样可以在配置阶段就发现问题，避免运行时异常。

**触发场景**：
1. **保存表映射时检查**（主要场景）：在 `TableGroupService.add()` 或 `TableGroupService.edit()` 时，检查目标表是否存在
2. **运行时兜底检查**（可选）：在 DDL 执行前或数据写入失败时检查（作为兜底机制）

**优势**：
- ✅ **提前发现问题**：在配置阶段就发现表不存在，而不是运行时
- ✅ **用户体验更好**：用户可以在配置时主动选择是否创建表，明确知道会发生什么
- ✅ **简化运行时逻辑**：同步过程中不需要检查表是否存在，减少性能开销
- ✅ **更符合预期**：用户明确知道表会被创建，而不是在运行时"偷偷"创建

#### 3.3.2 实现策略

**方案一：配置时检查 + 用户确认（推荐）**

在 `TableGroupChecker.checkAddConfigModel()` 中检查目标表是否存在：
- 如果目标表不存在，返回特殊结果，前端提示用户是否创建
- 用户确认后，调用创建表接口，创建成功后再继续保存表映射
- 用户取消时，直接抛出异常，保存映射关系失败
- 优点：提前发现问题，用户体验好，运行时逻辑简单，无需额外配置
- 缺点：需要前端交互支持

**方案二：运行时自动创建（兜底方案）**

在 DDL 处理流程或数据写入流程中，捕获"表不存在"异常后自动创建：
- 优点：覆盖所有场景，无需用户交互
- 缺点：运行时才发现问题，可能影响同步性能

**建议**：优先实现方案一，方案二作为兜底机制。

#### 3.3.3 配置时检查实现

**后端实现**：

在 `TableGroupChecker.checkAddConfigModel()` 中增加表存在性检查：

```java
@Override
public ConfigModel checkAddConfigModel(Map<String, String> params) throws Exception {
    // ... 现有逻辑 ...
    
    Mapping mapping = profileComponent.getMapping(mappingId);
    Assert.notNull(mapping, "mapping can not be null.");
    
    // 获取连接器信息
    TableGroup tableGroup = TableGroup.create(mappingId, sourceTable, parserComponent, profileComponent);
    tableGroup.setSourceTable(getTable(mapping.getSourceConnectorId(), sourceTable, sourceTablePK));
    
    // 检查目标表是否存在
    try {
        tableGroup.setTargetTable(getTable(mapping.getTargetConnectorId(), targetTable, targetTablePK));
    } catch (Exception e) {
        // 目标表不存在，抛出特殊异常，让前端提示用户是否创建
        throw new TargetTableNotExistsException(
            "目标表不存在: " + targetTable + 
            "，是否基于源表结构自动创建？", 
            mapping.getSourceConnectorId(),
            mapping.getTargetConnectorId(),
            sourceTable,
            targetTable
        );
    }
    
    // ... 后续逻辑 ...
}
```

**新增异常类**：

```java
package org.dbsyncer.biz;

/**
 * 目标表不存在异常
 * 用于标识目标表不存在，需要用户确认是否创建
 */
public class TargetTableNotExistsException extends BizException {
    
    /**
     * 错误码：用于前端识别异常类型
     */
    public static final String ERROR_CODE = "TARGET_TABLE_NOT_EXISTS";
    
    private String sourceConnectorId;
    private String targetConnectorId;
    private String sourceTable;
    private String targetTable;
    
    public TargetTableNotExistsException(String message, String sourceConnectorId, 
                                         String targetConnectorId, String sourceTable, String targetTable) {
        super(message);
        this.sourceConnectorId = sourceConnectorId;
        this.targetConnectorId = targetConnectorId;
        this.sourceTable = sourceTable;
        this.targetTable = targetTable;
    }
    
    public String getErrorCode() {
        return ERROR_CODE;
    }
    
    public String getSourceConnectorId() {
        return sourceConnectorId;
    }
    
    public String getTargetConnectorId() {
        return targetConnectorId;
    }
    
    public String getSourceTable() {
        return sourceTable;
    }
    
    public String getTargetTable() {
        return targetTable;
    }
}
```

**Controller 层异常处理改造**：

在 `TableGroupController.add()` 中识别 `TargetTableNotExistsException`：

```java
@PostMapping(value = "/add")
@ResponseBody
public RestResult add(HttpServletRequest request) {
    try {
        Map<String, String> params = getParams(request);
        return RestResult.restSuccess(tableGroupService.add(params));
    } catch (TargetTableNotExistsException e) {
        // 目标表不存在异常，返回特殊错误码和详细信息
        logger.info("目标表不存在，等待用户确认: {}", e.getTargetTable());
        Map<String, Object> errorInfo = new HashMap<>();
        errorInfo.put("errorCode", TargetTableNotExistsException.ERROR_CODE);
        errorInfo.put("message", e.getMessage());
        errorInfo.put("sourceConnectorId", e.getSourceConnectorId());
        errorInfo.put("targetConnectorId", e.getTargetConnectorId());
        errorInfo.put("sourceTable", e.getSourceTable());
        errorInfo.put("targetTable", e.getTargetTable());
        return RestResult.restFail(errorInfo, 400);
    } catch (SdkException e) {
        logger.error(e.getLocalizedMessage(), e);
        return RestResult.restFail(e.getMessage(), 400);
    } catch (Exception e) {
        logger.error(e.getLocalizedMessage(), e);
        return RestResult.restFail(e.getMessage());
    }
}
```

**新增创建表接口**：

在 `TableGroupController` 中新增创建表接口：

```java
@PostMapping(value = "/createTargetTable")
@ResponseBody
public RestResult createTargetTable(HttpServletRequest request) {
    try {
        Map<String, String> params = getParams(request);
        String mappingId = params.get("mappingId");
        String sourceTable = params.get("sourceTable");
        String targetTable = params.get("targetTable");
        
        Assert.hasText(mappingId, "mappingId 不能为空");
        Assert.hasText(sourceTable, "sourceTable 不能为空");
        Assert.hasText(targetTable, "targetTable 不能为空");
        
        Mapping mapping = profileComponent.getMapping(mappingId);
        Assert.notNull(mapping, "Mapping 不存在: " + mappingId);
        
        // 获取连接器配置
        ConnectorConfig sourceConfig = getConnectorConfig(mapping.getSourceConnectorId());
        ConnectorConfig targetConfig = getConnectorConfig(mapping.getTargetConnectorId());
        
        // 连接源和目标数据库
        ConnectorInstance sourceConnectorInstance = connectorFactory.connect(sourceConfig);
        ConnectorInstance targetConnectorInstance = connectorFactory.connect(targetConfig);
        
        // 检查目标表是否已存在（避免重复创建）
        try {
            MetaInfo existingTable = connectorFactory.getMetaInfo(targetConnectorInstance, targetTable);
            if (existingTable != null && existingTable.getColumn() != null && !existingTable.getColumn().isEmpty()) {
                return RestResult.restSuccess("目标表已存在，无需创建");
            }
        } catch (Exception e) {
            // 表不存在，继续创建流程
            logger.debug("目标表不存在，开始创建: {}", targetTable);
        }
        
        // 获取源表结构
        MetaInfo sourceMetaInfo = connectorFactory.getMetaInfo(sourceConnectorInstance, sourceTable);
        Assert.notNull(sourceMetaInfo, "无法获取源表结构: " + sourceTable);
        Assert.notEmpty(sourceMetaInfo.getColumn(), "源表没有字段: " + sourceTable);
        
        // 检查连接器是否支持生成 CREATE TABLE DDL
        ConnectorService targetConnectorService = connectorFactory.getConnectorService(targetConfig.getConnectorType());
        try {
            // 尝试生成 CREATE TABLE DDL
            String createTableDDL = targetConnectorService.generateCreateTableDDL(sourceMetaInfo, targetTable);
            Assert.hasText(createTableDDL, "无法生成 CREATE TABLE DDL");
            
            // 执行 CREATE TABLE DDL
            DDLConfig ddlConfig = new DDLConfig();
            ddlConfig.setSql(createTableDDL);
            Result result = connectorFactory.writerDDL(targetConnectorInstance, ddlConfig);
            
            if (result.hasError()) {
                logger.error("创建表失败: {}", result.getError());
                return RestResult.restFail("创建表失败: " + result.getError(), 500);
            }
            
            logger.info("成功创建目标表: {}", targetTable);
            return RestResult.restSuccess("创建表成功");
            
        } catch (UnsupportedOperationException e) {
            logger.error("连接器不支持自动生成 CREATE TABLE DDL: {}", targetConfig.getConnectorType());
            return RestResult.restFail("该数据库类型不支持自动创建表: " + targetConfig.getConnectorType(), 400);
        }
        
    } catch (IllegalArgumentException e) {
        logger.error("参数错误: {}", e.getMessage());
        return RestResult.restFail("参数错误: " + e.getMessage(), 400);
    } catch (Exception e) {
        logger.error("创建表异常: {}", e.getMessage(), e);
        return RestResult.restFail("创建表失败: " + e.getMessage(), 500);
    }
}
```

**前端实现**：

在 `edit.js` 中修改 `bindMappingTableGroupAddClick()` 函数：

```javascript
function bindMappingTableGroupAddClick($sourceSelect, $targetSelect) {
    let $addBtn = $("#tableGroupAddBtn");
    $addBtn.unbind("click");
    $addBtn.bind('click', function () {
        // ... 现有参数收集逻辑 ...
        
        doPoster("/tableGroup/add", m, function (data) {
            if (data.success == true) {
                bootGrowl("新增映射关系成功!", "success");
                refresh(m.mappingId, 1);
            } else {
                // 检查是否是目标表不存在的异常（通过错误码识别）
                if (data.status == 400 && data.resultValue && 
                    typeof data.resultValue === 'object' &&
                    data.resultValue.errorCode === 'TARGET_TABLE_NOT_EXISTS') {
                    // 显示确认对话框
                    showCreateTableConfirmDialog(m, data.resultValue);
                } else {
                    // 其他错误，直接显示错误信息
                    let errorMsg = typeof data.resultValue === 'string' 
                        ? data.resultValue 
                        : (data.resultValue.message || '操作失败');
                    bootGrowl(errorMsg, "danger");
                    if (data.status == 400) {
                        refresh(m.mappingId, 1);
                    }
                }
            }
        });
    });
}

/**
 * 显示创建表确认对话框
 * @param params 保存表映射的参数
 * @param errorInfo 错误信息对象，包含 errorCode, message, sourceTable, targetTable 等
 */
function showCreateTableConfirmDialog(params, errorInfo) {
    // 使用 BootstrapDialog（项目中实际使用的对话框组件）
    BootstrapDialog.show({
        title: "目标表不存在",
        type: BootstrapDialog.TYPE_WARNING,
        message: '<div style="padding: 10px;">' +
                 '<p><strong>目标表不存在：</strong>' + errorInfo.targetTable + '</p>' +
                 '<p>是否基于源表结构自动创建目标表？</p>' +
                 '<p style="color: #999; font-size: 12px;">源表：' + errorInfo.sourceTable + '</p>' +
                 '</div>',
        size: BootstrapDialog.SIZE_NORMAL,
        buttons: [{
            label: "创建",
            cssClass: "btn-primary",
            action: function (dialog) {
                dialog.close();
                // 用户确认创建表
                createTargetTableAndRetry(params, errorInfo);
            }
        }, {
            label: "取消",
            cssClass: "btn-default",
            action: function (dialog) {
                dialog.close();
                bootGrowl("已取消创建表", "info");
            }
        }]
    });
}

/**
 * 创建表并重试保存映射关系
 * @param params 保存表映射的参数
 * @param errorInfo 错误信息对象
 */
function createTargetTableAndRetry(params, errorInfo) {
    // 显示加载提示
    bootGrowl("正在创建目标表...", "info");
    
    // 1. 先创建表
    let createParams = {
        mappingId: params.mappingId,
        sourceTable: errorInfo.sourceTable,
        targetTable: errorInfo.targetTable
    };
    
    doPoster("/tableGroup/createTargetTable", createParams, function (data) {
        if (data.success == true) {
            bootGrowl("创建表成功，正在保存映射关系...", "success");
            
            // 2. 创建成功后，重新尝试保存表映射
            doPoster("/tableGroup/add", params, function (data) {
                if (data.success == true) {
                    bootGrowl("新增映射关系成功!", "success");
                    refresh(params.mappingId, 1);
                } else {
                    // 保存映射关系失败
                    let errorMsg = typeof data.resultValue === 'string' 
                        ? data.resultValue 
                        : (data.resultValue.message || '保存映射关系失败');
                    bootGrowl(errorMsg, "danger");
                }
            });
        } else {
            // 创建表失败
            let errorMsg = typeof data.resultValue === 'string' 
                ? data.resultValue 
                : (data.resultValue.message || '创建表失败');
            bootGrowl("创建表失败: " + errorMsg, "danger");
        }
    });
}
```

**多表场景处理**：

如果同时保存多个表映射（`sourceTable` 和 `targetTable` 包含多个表，用 `|` 分隔），需要逐个处理：

```javascript
function bindMappingTableGroupAddClick($sourceSelect, $targetSelect) {
    // ... 现有逻辑 ...
    
    doPoster("/tableGroup/add", m, function (data) {
        if (data.success == true) {
            bootGrowl("新增映射关系成功!", "success");
            refresh(m.mappingId, 1);
        } else {
            // 检查是否是目标表不存在的异常
            if (data.status == 400 && data.resultValue && 
                typeof data.resultValue === 'object' &&
                data.resultValue.errorCode === 'TARGET_TABLE_NOT_EXISTS') {
                
                // 多表场景：检查是否有多个表需要创建
                let sourceTables = m.sourceTable.split('|');
                let targetTables = m.targetTable.split('|');
                
                if (sourceTables.length > 1 || targetTables.length > 1) {
                    // 多个表，需要批量处理
                    showBatchCreateTableConfirmDialog(m, data.resultValue, sourceTables, targetTables);
                } else {
                    // 单个表，直接显示确认对话框
                    showCreateTableConfirmDialog(m, data.resultValue);
                }
            } else {
                // 其他错误...
            }
        }
    });
}

/**
 * 批量创建表确认对话框（多表场景）
 */
function showBatchCreateTableConfirmDialog(params, errorInfo, sourceTables, targetTables) {
    let tableListHtml = '<ul style="margin: 10px 0; padding-left: 20px;">';
    for (let i = 0; i < targetTables.length; i++) {
        tableListHtml += '<li>源表: ' + sourceTables[i] + ' → 目标表: ' + targetTables[i] + '</li>';
    }
    tableListHtml += '</ul>';
    
    BootstrapDialog.show({
        title: "目标表不存在",
        type: BootstrapDialog.TYPE_WARNING,
        message: '<div style="padding: 10px;">' +
                 '<p><strong>以下目标表不存在：</strong></p>' +
                 tableListHtml +
                 '<p>是否基于源表结构自动创建这些目标表？</p>' +
                 '</div>',
        size: BootstrapDialog.SIZE_NORMAL,
        buttons: [{
            label: "全部创建",
            cssClass: "btn-primary",
            action: function (dialog) {
                dialog.close();
                batchCreateTargetTablesAndRetry(params, sourceTables, targetTables);
            }
        }, {
            label: "取消",
            cssClass: "btn-default",
            action: function (dialog) {
                dialog.close();
                bootGrowl("已取消创建表", "info");
            }
        }]
    });
}

/**
 * 批量创建表并重试保存映射关系
 */
function batchCreateTargetTablesAndRetry(params, sourceTables, targetTables) {
    bootGrowl("正在批量创建目标表...", "info");
    
    let createPromises = [];
    for (let i = 0; i < targetTables.length; i++) {
        let createParams = {
            mappingId: params.mappingId,
            sourceTable: sourceTables[i],
            targetTable: targetTables[i]
        };
        
        // 创建 Promise 来顺序执行创建表操作
        createPromises.push(function() {
            return new Promise(function(resolve, reject) {
                doPoster("/tableGroup/createTargetTable", createParams, function (data) {
                    if (data.success == true) {
                        resolve({table: targetTables[i], success: true});
                    } else {
                        reject({table: targetTables[i], error: data.resultValue});
                    }
                });
            });
        });
    }
    
    // 顺序执行所有创建表操作
    let promiseChain = createPromises.reduce(function(chain, promiseFn) {
        return chain.then(function() {
            return promiseFn();
        });
    }, Promise.resolve());
    
    promiseChain.then(function() {
        bootGrowl("所有表创建成功，正在保存映射关系...", "success");
        // 重新尝试保存表映射
        doPoster("/tableGroup/add", params, function (data) {
            if (data.success == true) {
                bootGrowl("新增映射关系成功!", "success");
                refresh(params.mappingId, 1);
            } else {
                bootGrowl("保存映射关系失败: " + data.resultValue, "danger");
            }
        });
    }).catch(function(error) {
        bootGrowl("创建表失败: " + error.table + " - " + error.error, "danger");
    });
}
```

#### 3.3.4 CREATE TABLE DDL 生成

**实现状态**：`generateCreateTableDDL()` 方法目前**尚未在 `ConnectorService` 接口中定义**，需要：

1. **扩展 `ConnectorService` 接口**：添加 `generateCreateTableDDL()` 方法定义
2. **各连接器实现**：每个数据库连接器都需要实现该方法

**接口扩展**：

在 `ConnectorService` 接口中添加方法定义：

```java
// 在 dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/spi/ConnectorService.java 中添加
public interface ConnectorService<I extends ConnectorInstance, C extends ConnectorConfig> {
    
    // ... 现有方法 ...
    
    /**
     * 基于源表结构生成目标表的 CREATE TABLE DDL
     * 
     * @param sourceMetaInfo 源表元信息（字段为标准类型）
     * @param targetTableName 目标表名
     * @return CREATE TABLE DDL 语句
     * @throws UnsupportedOperationException 如果连接器不支持此功能
     */
    default String generateCreateTableDDL(MetaInfo sourceMetaInfo, String targetTableName) {
        // 默认实现：抛出未实现异常
        throw new UnsupportedOperationException("该连接器不支持自动生成 CREATE TABLE DDL: " + getConnectorType());
    }
}
```

**核心实现思路：复用现有 SqlTemplate.convertToDatabaseType() 方法**

**重要**：**不需要重新实现类型格式化逻辑**，应该直接复用现有的 `SqlTemplate.convertToDatabaseType()` 方法：

1. **类型转换**：使用 `SchemaResolver.fromStandardType()` 将标准类型转换为目标数据库类型（已有）
2. **DDL 格式化**：使用 `SqlTemplate.convertToDatabaseType()` 格式化 DDL 类型字符串（**已存在，直接复用**）
3. **语法差异**：处理不同数据库的语法差异（表名引号、主键定义等）

**现有实现说明**：

`SqlTemplate.convertToDatabaseType(Field column)` 方法已经在 DDL 同步过程中使用，用于将标准类型字段转换为数据库特定的类型字符串（包含长度、精度等）。各数据库的 Template 类都已实现：

- `MySQLTemplate.convertToDatabaseType()` - MySQL 类型格式化
- `SqlServerTemplate.convertToDatabaseType()` - SQL Server 类型格式化
- `PostgreSQLTemplate.convertToDatabaseType()` - PostgreSQL 类型格式化
- `OracleTemplate.convertToDatabaseType()` - Oracle 类型格式化

**连接器实现模式（复用现有方法）**：

```java
public final class MySQLConnector extends AbstractDatabaseConnector {
    
    @Override
    public String generateCreateTableDDL(MetaInfo sourceMetaInfo, String targetTableName) {
        SqlTemplate sqlTemplate = getSqlTemplate();
        if (sqlTemplate == null) {
            throw new UnsupportedOperationException("MySQL连接器不支持自动生成 CREATE TABLE DDL");
        }
        
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE `").append(targetTableName).append("` (\n");
        
        List<String> columnDefs = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();
        
        for (Field sourceField : sourceMetaInfo.getColumn()) {
            // 1. 直接使用 SqlTemplate.convertToDatabaseType() 方法
            //    该方法内部已经处理了类型转换和格式化（包括长度、精度等）
            String ddlType = sqlTemplate.convertToDatabaseType(sourceField);
            
            // 2. 构建列定义
            String columnDef = String.format("  `%s` %s", sourceField.getName(), ddlType);
            columnDefs.add(columnDef);
            
            // 3. 收集主键
            if (sourceField.isPk()) {
                primaryKeys.add("`" + sourceField.getName() + "`");
            }
        }
        
        ddl.append(String.join(",\n", columnDefs));
        
        if (!primaryKeys.isEmpty()) {
            ddl.append(",\n  PRIMARY KEY (").append(String.join(", ", primaryKeys)).append(")");
        }
        
        ddl.append("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        return ddl.toString();
    }
}
```

**关键优势**：

1. **零重复代码**：直接复用现有的 `SqlTemplate.convertToDatabaseType()` 方法
2. **已充分测试**：该方法已在 DDL 同步过程中使用，经过充分验证
3. **逻辑一致**：CREATE TABLE DDL 生成与 ALTER TABLE DDL 生成使用相同的类型格式化逻辑
4. **无需扩展接口**：不需要在 `SchemaResolver` 中添加新方法

**各数据库连接器的语法差异**：

| 数据库 | 表名引号 | 列名引号 | 主键定义 | 特殊语法 |
|--------|---------|---------|---------|---------|
| **MySQL** | 反引号 `` ` `` | 反引号 `` ` `` | `PRIMARY KEY (...)` | `ENGINE=InnoDB DEFAULT CHARSET=utf8mb4` |
| **SQL Server** | 方括号 `[]` | 方括号 `[]` | `PRIMARY KEY (...)` | 无 |
| **PostgreSQL** | 双引号 `"` | 双引号 `"` | `PRIMARY KEY (...)` | 无 |
| **Oracle** | 双引号 `"` | 双引号 `"` | `PRIMARY KEY (...)` | 无 |

**实施建议**：

1. **直接复用现有方法**：使用 `SqlTemplate.convertToDatabaseType()` 方法，该方法已在 DDL 同步中使用
2. **优先实现常用连接器**：MySQL、SQL Server、PostgreSQL 连接器实现 `generateCreateTableDDL()` 方法
3. **复用现有基础设施**：
   - `SqlTemplate.convertToDatabaseType()`：类型转换和格式化（**已存在，直接使用**）
   - `SqlTemplate.buildColumn()`：列名引号处理（**已存在，直接使用**）
4. **统一实现模式**：所有连接器遵循相同的实现模式（字段定义、主键处理），类型格式化逻辑统一在 SqlTemplate 中
5. **错误处理**：如果连接器不支持，返回明确的错误提示

**注意事项**：

- **必须复用 `SqlTemplate.convertToDatabaseType()` 方法**：
  - 该方法内部已经调用了 `SchemaResolver.fromStandardType()` 进行类型转换
  - 该方法已经处理了长度、精度等格式化逻辑
  - 该方法已经在 DDL 同步过程中使用，经过充分验证
- **避免重复实现**：不要重新实现类型格式化逻辑，直接使用现有方法
- **语法差异**：不同数据库的表名引号、列名引号、主键定义语法不同，需要在连接器实现中处理
- **类型完整性**：`SqlTemplate.convertToDatabaseType()` 已经处理了所有标准类型的转换和格式化，包括特殊类型（如 Geometry、JSON 等）
- **扩展性**：新增类型时，只需在对应的 SqlTemplate 中更新 `convertToDatabaseType()` 方法，无需修改连接器代码

### 3.4 配置管理改造

#### 3.4.1 MappingChecker 改造

在 `MappingChecker.updateListenerConfig()` 方法中增加新配置项的解析：

```java
private void updateListenerConfig(ListenerConfig listener, Map<String, String> params) {
    Assert.notNull(listener, "ListenerConfig can not be null.");
    
    // 现有配置
    listener.setEnableUpdate(StringUtil.isNotBlank(params.get("enableUpdate")));
    listener.setEnableInsert(StringUtil.isNotBlank(params.get("enableInsert")));
    listener.setEnableDelete(StringUtil.isNotBlank(params.get("enableDelete")));
    listener.setEnableDDL(StringUtil.isNotBlank(params.get("enableDDL")));
    
    // 新增 DDL 细粒度配置
    // 注意：使用 StringUtil.isNotBlank 判断，空字符串视为 false
    // 如果参数不存在，使用缺省值（已在 ListenerConfig 中设置）
    if (params.containsKey("allowAddColumn")) {
        listener.setAllowAddColumn(StringUtil.isNotBlank(params.get("allowAddColumn")));
    }
    if (params.containsKey("allowDropColumn")) {
        listener.setAllowDropColumn(StringUtil.isNotBlank(params.get("allowDropColumn")));
    }
    if (params.containsKey("allowModifyColumn")) {
        listener.setAllowModifyColumn(StringUtil.isNotBlank(params.get("allowModifyColumn")));
    }
}
```

#### 3.4.2 前端配置界面改造

在 `editIncrement.html` 中增加 DDL 细粒度配置选项：

```html
<!-- DDL 配置区域 -->
<div class="form-group">
    <div class="row">
        <div class="col-md-4">
            <label class="col-sm-3 control-label text-right">DDL</label>
            <div class="col-sm-9">
                <input name="enableDDL" class="dbsyncer_switch" 
                       th:checked="${mapping?.listener?.enableDDL}" type="checkbox">
            </div>
        </div>
        <div class="col-md-8"></div>
    </div>
</div>

<!-- DDL 细粒度配置（仅在 enableDDL 为 true 时显示） -->
<div class="form-group" id="ddlDetailConfig" th:style="${mapping?.listener?.enableDDL} ? '' : 'display:none;'">
    <div class="row">
        <div class="col-md-3">
            <label class="col-sm-4 control-label text-right">允许加字段</label>
            <div class="col-sm-8">
                <input name="allowAddColumn" class="dbsyncer_switch" 
                       th:checked="${mapping?.listener?.allowAddColumn != null ? mapping?.listener?.allowAddColumn : true}" 
                       type="checkbox">
            </div>
        </div>
        <div class="col-md-3">
            <label class="col-sm-4 control-label text-right">允许删字段</label>
            <div class="col-sm-8">
                <input name="allowDropColumn" class="dbsyncer_switch" 
                       th:checked="${mapping?.listener?.allowDropColumn != null ? mapping?.listener?.allowDropColumn : true}" 
                       type="checkbox">
            </div>
        </div>
        <div class="col-md-3">
            <label class="col-sm-4 control-label text-right">允许改字段</label>
            <div class="col-sm-8">
                <input name="allowModifyColumn" class="dbsyncer_switch" 
                       th:checked="${mapping?.listener?.allowModifyColumn != null ? mapping?.listener?.allowModifyColumn : true}" 
                       type="checkbox">
            </div>
        </div>
    </div>
</div>

<script>
// 当 enableDDL 开关变化时，显示/隐藏细粒度配置
$('input[name="enableDDL"]').on('change', function() {
    if ($(this).is(':checked')) {
        $('#ddlDetailConfig').show();
    } else {
        $('#ddlDetailConfig').hide();
    }
});
</script>
```

## 四、实现步骤

### 4.1 第一阶段：配置结构扩展

1. **扩展 ListenerConfig 类**
   - 新增 4 个配置字段及 getter/setter
   - 设置缺省值为 `true`

2. **扩展 MappingChecker**
   - 在 `updateListenerConfig()` 中增加新配置项解析

3. **前端界面改造**
   - 在 `editIncrement.html` 中增加配置选项
   - 实现配置项的显示/隐藏逻辑

### 4.2 第二阶段：表自动创建实现（配置时检查）

1. **TableGroupChecker 改造**
   - 在 `checkAddConfigModel()` 中增加目标表存在性检查
   - 新增 `TargetTableNotExistsException` 异常类
   - 当目标表不存在时，抛出特殊异常，前端提示用户是否创建

2. **ConnectorService 接口扩展**
   - 在 `ConnectorService` 接口中添加 `generateCreateTableDDL()` 方法定义
   - 提供默认实现（抛出 `UnsupportedOperationException`）

3. **创建表接口实现**
   - 在 `TableGroupController` 中新增 `createTargetTable()` 接口
   - 实现基于源表结构生成目标表 DDL 的逻辑
   - 处理连接器不支持的情况

4. **各连接器实现 `generateCreateTableDDL()` 方法**
   - **MySQL 连接器**：实现 MySQL 语法（反引号、ENGINE、CHARSET）
   - **SQL Server 连接器**：实现 SQL Server 语法（方括号）
   - **PostgreSQL 连接器**：实现 PostgreSQL 语法（双引号）
   - **Oracle 连接器**：实现 Oracle 语法（双引号、特殊类型）
   - 参考本文档第 3.3.4 节中的实现模式
   - 其他连接器（Kafka、Elasticsearch 等）保持默认实现（不支持）

3. **前端交互实现**
   - 修改 `edit.js` 中的 `bindMappingTableGroupAddClick()` 函数
   - 实现表不存在时的确认对话框
   - 实现创建表后重试保存映射关系的流程

### 4.3 第三阶段：DDL 处理逻辑改造

1. **GeneralBufferActuator 改造**
   - 在 `parseDDl()` 中增加配置检查逻辑
   - 实现 `isDDLOperationAllowed()` 方法
   - 移除运行时表存在性检查（已在配置时完成）

2. **运行时兜底机制（可选）**
   - 在数据写入失败时检查是否为"表不存在"异常
   - 如果允许自动创建，记录警告日志（建议用户检查配置）

### 4.4 第四阶段：测试与验证

1. **单元测试**
   - 测试配置项缺省值
   - 测试配置项解析逻辑
   - 测试 DDL 操作过滤逻辑

2. **集成测试**
   - 测试不同配置组合下的 DDL 同步行为
   - 测试表自动创建功能
   - 测试配置兼容性（旧配置迁移）

## 五、配置示例

### 5.1 JSON 配置示例

```json
{
  "listener": {
    "listenerType": "log",
    "enableDDL": true,
    "allowAddColumn": true,
    "allowDropColumn": false,
    "allowModifyColumn": true,
    "enableInsert": true,
    "enableUpdate": true,
    "enableDelete": true
  }
}
```

### 5.2 配置场景示例

**场景一：只允许新增字段，不允许删除和修改**
```json
{
  "enableDDL": true,
  "allowAddColumn": true,
  "allowDropColumn": false,
  "allowModifyColumn": false
}
```

**场景二：允许所有 DDL 操作**
```json
{
  "enableDDL": true,
  "allowAddColumn": true,
  "allowDropColumn": true,
  "allowModifyColumn": true
}
```

**注意**：表自动创建功能在保存表映射时自动触发，无需配置项控制。如果目标表不存在，系统会提示用户是否创建。

**场景三：完全禁用 DDL 同步**
```json
{
  "enableDDL": false
  // 其他 DDL 配置项不生效
}
```

## 六、注意事项

### 6.1 配置优先级

1. **全局开关优先**：`enableDDL = false` 时，所有 DDL 操作均被禁用
2. **细粒度控制**：`enableDDL = true` 时，细粒度配置生效
3. **缺省值策略**：配置项缺失时使用缺省值（`true`）

### 6.2 字段映射更新

- 当 DDL 操作被配置禁用时，**不执行 DDL**，但**不更新字段映射**
- 这意味着字段映射可能与实际表结构不一致
- **建议**：在配置变更时，触发一次表结构同步，确保字段映射一致性

### 6.3 表自动创建机制

- **自动触发**：在保存表映射时自动检查目标表是否存在，无需配置项控制
- **用户确认**：如果目标表不存在，系统会提示用户是否创建，用户可以选择创建或取消
- **连接器支持**：表自动创建功能需要目标数据库连接器支持 `generateCreateTableDDL()` 方法
- **权限限制**：表自动创建可能受到数据库权限限制，创建失败时会提示用户
- **多表场景**：如果同时保存多个表映射，需要逐个确认创建（或提供批量确认选项）
- **运行时兜底**：如果配置时未创建表，运行时不会自动创建（避免意外操作）

### 6.4 错误处理机制

- **异常类型识别**：使用错误码 `TARGET_TABLE_NOT_EXISTS` 而非错误消息文本识别异常类型，提高可靠性
- **错误信息传递**：通过 `RestResult` 的 `resultValue` 传递结构化错误信息（包含错误码、消息、上下文）
- **前端错误处理**：前端根据错误码进行不同处理，而非依赖字符串匹配
- **创建表失败处理**：创建表失败时，显示具体错误信息，不自动重试
- **保存映射失败处理**：创建表成功但保存映射失败时，提示用户手动重试
- **连接器不支持处理**：如果连接器不支持 `generateCreateTableDDL()`，返回明确的错误提示

### 6.5 向后兼容性

- 现有配置（仅包含 `enableDDL`）完全兼容
- 新增配置项使用缺省值，不影响现有功能
- 配置迁移无需特殊处理

## 七、扩展性考虑

### 7.1 未来可能的扩展

1. **字段重命名单独控制**：新增 `allowRenameColumn` 配置项
2. **索引变更控制**：新增 `allowCreateIndex`、`allowDropIndex` 等配置项
3. **约束变更控制**：新增 `allowAddConstraint`、`allowDropConstraint` 等配置项
4. **表级操作控制**：新增 `allowRenameTable`、`allowTruncateTable` 等配置项

### 7.2 配置分组

如果未来配置项增多，可以考虑配置分组：

```java
public class DDLConfig {
    private boolean enableDDL;
    private TableDDLConfig tableConfig;      // 表级操作配置
    private ColumnDDLConfig columnConfig;   // 列级操作配置
    private IndexDDLConfig indexConfig;     // 索引操作配置
}
```

## 八、实施细节总结

### 8.1 前端如何感知目标表不存在异常

**识别机制**：
1. 后端抛出 `TargetTableNotExistsException` 异常
2. Controller 层捕获异常，返回包含错误码的 `RestResult`
3. 前端通过 `data.resultValue.errorCode === 'TARGET_TABLE_NOT_EXISTS'` 识别异常

**优势**：
- 不依赖错误消息文本，提高可靠性
- 错误码可扩展，便于未来增加新的异常类型
- 结构化错误信息，包含完整的上下文

### 8.2 用户交互界面

**对话框组件**：使用项目现有的 `BootstrapDialog` 组件

**交互流程**：
1. 用户点击"新增映射关系"
2. 后端检测到目标表不存在，返回错误码
3. 前端显示确认对话框，包含：
   - 标题："目标表不存在"
   - 消息：显示源表和目标表信息
   - 按钮："创建"（主按钮）和"取消"（默认按钮）
4. 用户确认后，显示加载提示，调用创建表接口
5. 创建成功后，自动重试保存映射关系

**多表场景**：
- 显示所有需要创建的表列表
- 提供"全部创建"按钮，顺序创建所有表
- 如果某个表创建失败，显示具体错误信息

### 8.3 服务端自动创建目标表处理

**处理流程**：
1. **参数校验**：检查 mappingId、sourceTable、targetTable 是否为空
2. **连接器验证**：验证 Mapping 和连接器是否存在
3. **表存在性检查**：检查目标表是否已存在（避免重复创建）
4. **源表结构获取**：从源数据库获取表结构（MetaInfo）
5. **DDL 生成**：调用连接器的 `generateCreateTableDDL()` 生成 CREATE TABLE 语句
6. **连接器支持检查**：如果连接器不支持，返回明确错误
7. **执行 DDL**：调用 `connectorFactory.writerDDL()` 执行创建表操作
8. **错误处理**：捕获并返回详细的错误信息

**错误处理**：
- 参数错误：返回 400 状态码
- 连接器不支持：返回 400 状态码，明确提示
- 创建表失败：返回 500 状态码，包含具体错误信息
- 权限不足：通过 DDL 执行结果返回具体错误

### 8.4 关键实施点

1. **异常类定义**：`TargetTableNotExistsException` 必须包含错误码常量
2. **Controller 改造**：`TableGroupController.add()` 必须识别并特殊处理该异常
3. **前端识别**：必须通过错误码而非字符串匹配识别异常
4. **对话框组件**：使用 `BootstrapDialog` 而非 `bootbox`
5. **多表处理**：支持批量创建和顺序执行
6. **错误提示**：所有错误都要有明确的用户提示

## 九、相关文档

- [DDL 同步文档](./ddl.md)
- [SQL Server Change Tracking 同步方案](./mssql-ct.md)
- [异构数据库 DDL 同步实施方案](./ddl-heterogeneous.md)

**注意**：`doc/create-missing-table.md` 文档已废弃，相关内容已整合到本文档第 3.3.4 节。

