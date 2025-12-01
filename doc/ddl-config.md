# DDL 同步配置方案

## 一、概述

为了满足 DDL 同步的精细化控制需求，在现有 `enableDDL` 配置基础上，新增细粒度的 DDL 行为控制配置项。这些配置项用于约束和控制不同类型的 DDL 操作，提供更灵活的同步策略。

## 二、需求分析

### 2.1 现有配置限制

当前 DDL 同步仅通过 `ListenerConfig.enableDDL` 进行全局开关控制，无法细粒度控制：
- 无法区分不同类型的 DDL 操作（ADD、DROP、MODIFY、CHANGE）
- 无法控制表自动创建行为
- 字段映射机制在 DDL 同步时缺乏灵活的控制策略

### 2.2 新增配置需求

根据业务场景，需要支持以下配置项：

1. **自动创建缺失的数据表**（`autoCreateTable`）
   - 缺省值：`true`
   - 用途：当目标表不存在时，是否自动基于源表结构创建目标表

2. **允许加字段DDL**（`allowAddColumn`）
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
     * 自动创建缺失的数据表
     * 缺省值：true
     */
    private boolean autoCreateTable = true;
    
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
        
        // 4. 检查目标表是否存在（用于自动创建表）
        ConnectorInstance tConnectorInstance = connectorFactory.connect(tConnConfig);
        boolean tableExists = checkTableExists(tConnectorInstance, tableGroup.getTargetTable().getName());
        
        if (!tableExists) {
            if (listenerConfig.isAutoCreateTable()) {
                // 自动创建表
                logger.info("目标表不存在，自动创建表: {}", tableGroup.getTargetTable().getName());
                createTargetTable(mapping, tableGroup, tConnectorInstance);
            } else {
                logger.error("目标表不存在且自动创建已禁用，跳过 DDL 执行。表: {}", 
                    tableGroup.getTargetTable().getName());
                return;
            }
        }
        
        // 5. 执行 DDL（原有逻辑）
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
    // 否则需要扩展 ConnectorService 接口（参考 doc/create-missing-table.md）
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

#### 3.3.1 触发时机

表自动创建在以下场景触发：
1. **DDL 执行前检查**：在执行任何 DDL 操作前，检查目标表是否存在
2. **数据同步异常**：当数据写入失败且错误为"表不存在"时（可选，参考 `doc/create-missing-table.md`）

#### 3.3.2 实现策略

**方案一：在 DDL 处理流程中集成（推荐）**

在 `parseDDl()` 方法中，执行 DDL 前检查表是否存在：
- 优点：逻辑集中，易于维护
- 缺点：仅适用于 DDL 同步场景

**方案二：在数据写入流程中集成（扩展方案）**

在 `AbstractDatabaseConnector.writer()` 方法中，捕获"表不存在"异常后自动创建：
- 优点：覆盖所有场景（DDL 同步、数据同步）
- 缺点：需要扩展 ConnectorService 接口，改动较大

**建议**：优先实现方案一，方案二作为后续扩展。

#### 3.3.3 CREATE TABLE DDL 生成

需要各数据库连接器实现 `generateCreateTableDDL()` 方法：

```java
// 在 ConnectorService 接口中扩展（如果尚未实现）
public interface ConnectorService<I extends ConnectorInstance, C extends ConnectorConfig> {
    
    /**
     * 基于源表结构生成目标表的 CREATE TABLE DDL
     * 
     * @param sourceMetaInfo 源表元信息
     * @param targetTableName 目标表名
     * @return CREATE TABLE DDL 语句
     */
    default String generateCreateTableDDL(MetaInfo sourceMetaInfo, String targetTableName) {
        // 默认实现：抛出未实现异常
        throw new UnsupportedOperationException("该连接器不支持自动生成 CREATE TABLE DDL");
    }
}
```

各数据库连接器实现示例（参考 `doc/create-missing-table.md`）：

- **MySQL**：生成 `CREATE TABLE \`table_name\` (...)`
- **SQL Server**：生成 `CREATE TABLE [table_name] (...)`
- **PostgreSQL**：生成 `CREATE TABLE "table_name" (...)`
- **Oracle**：生成 `CREATE TABLE "table_name" (...)`

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
    if (params.containsKey("autoCreateTable")) {
        listener.setAutoCreateTable(StringUtil.isNotBlank(params.get("autoCreateTable")));
    }
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
            <label class="col-sm-4 control-label text-right">自动建表</label>
            <div class="col-sm-8">
                <input name="autoCreateTable" class="dbsyncer_switch" 
                       th:checked="${mapping?.listener?.autoCreateTable != null ? mapping?.listener?.autoCreateTable : true}" 
                       type="checkbox">
            </div>
        </div>
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

### 4.2 第二阶段：DDL 处理逻辑改造

1. **GeneralBufferActuator 改造**
   - 在 `parseDDl()` 中增加配置检查逻辑
   - 实现 `isDDLOperationAllowed()` 方法
   - 实现 `checkTableExists()` 方法

2. **表自动创建实现**
   - 实现 `createTargetTable()` 方法
   - 扩展 ConnectorService 接口（如需要）
   - 实现各数据库连接器的 `generateCreateTableDDL()` 方法

### 4.3 第三阶段：测试与验证

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
    "autoCreateTable": true,
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

**场景二：允许所有 DDL 操作，但不自动创建表**
```json
{
  "enableDDL": true,
  "autoCreateTable": false,
  "allowAddColumn": true,
  "allowDropColumn": true,
  "allowModifyColumn": true
}
```

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

### 6.3 表自动创建限制

- 表自动创建功能需要目标数据库连接器支持 `generateCreateTableDDL()` 方法
- 如果连接器不支持，`autoCreateTable` 配置不生效，会记录警告日志
- 表自动创建可能受到数据库权限限制

### 6.4 向后兼容性

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

## 八、相关文档

- [DDL 同步文档](./ddl.md)
- [SQL Server Change Tracking 同步方案](./mssql-ct.md)
- [目标表不存在时自动创建功能设计方案](./create-missing-table.md)
- [异构数据库 DDL 同步实施方案](./ddl-heterogeneous.md)

