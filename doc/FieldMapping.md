# DBSyncer 字段映射类型标准化方案

## 问题背景

在异构数据同步过程中，SQL Server的`int identity`等数据库特定类型在跨数据库同步时会出现类型不兼容问题。

### 典型错误
```
org.dbsyncer.sdk.SdkException: MySQLSchemaResolver does not support type [class java.lang.Integer] convert to [int identity], val [501]
```

## 问题分析

### 根本原因
在`TableGroupChecker.setFieldMapping`方法中，当目标字段为空时，直接复制源字段信息，导致类型污染：

```java
// 问题场景：直接复制源字段，导致类型污染
if (null == t) {
    t = new Field(s.getName(), s.getTypeName(), s.getType(), s.isPk(), s.getColumnSize(), s.getRatio());
    // s.getTypeName() = "int identity" (SQL Server特定类型)
    // 结果：MySQL目标字段也变成了"int identity"，无法处理
}
```

## 解决方案

### 核心思路
在字段映射配置阶段，当目标字段为空时，使用源连接器的SchemaResolver将源字段标准化为标准类型，避免类型污染。

### 1. SchemaResolver接口扩展

```java
public interface SchemaResolver {
    /**
     * 类型合并：将数据库特定类型合并为Java标准类型
     */
    Object merge(Object val, Field field);
    
    /**
     * 类型转换：将标准类型转换为目标数据库特定类型
     */
    Object convert(Object val, Field field);
    
    /**
     * 字段类型标准化：将数据库特定字段转换为标准字段
     * 用于字段映射配置阶段的类型标准化
     */
    Field toStandardType(Field field);
}
```

### 2. 各连接器实现toStandardType方法

每个数据库连接器都需要实现真正的类型标准化：

```java
// SQL Server示例
@Override
public Field toStandardType(Field field) {
    DataType dataType = getDataType(field);
    if (dataType != null) {
        return new Field(field.getName(),
                       dataType.getType().name(),  // "int identity" → "INT"
                       getStandardTypeCode(dataType.getType()),
                       field.isPk(), field.getColumnSize(), field.getRatio());
    }
    throw new UnsupportedOperationException("Unsupported type: " + field.getTypeName());
}
```

### 3. TableGroupChecker核心修复

**关键修复**：在`setFieldMapping`方法中，当目标字段为空时，使用源连接器的SchemaResolver进行类型标准化：

```java
// 修复前：直接复制源字段，导致类型污染
if (null == t) {
    t = new Field(s.getName(), s.getTypeName(), s.getType(), s.isPk(), s.getColumnSize(), s.getRatio());
}

// 修复后：进行类型标准化
if (null == t) {
    // 获取源连接器的SchemaResolver进行类型标准化
    Mapping mapping = profileComponent.getMapping(tableGroup.getMappingId());
    ConnectorConfig sourceConnectorConfig = getConnectorConfig(mapping.getSourceConnectorId());
    ConnectorService<?, ?> sourceConnectorService = connectorFactory.getConnectorService(sourceConnectorConfig.getConnectorType());
    SchemaResolver sourceSchemaResolver = sourceConnectorService.getSchemaResolver();
    
    // 将源字段标准化为标准类型
    Field standardizedField = sourceSchemaResolver.toStandardType(s);
    t = standardizedField;
}
```

**修复效果**：
- SQL Server的`int identity` → 标准类型`INT`
- 避免了类型污染问题
- 确保目标字段使用标准类型，所有数据库都能处理

## 实施步骤

### 1. 扩展SchemaResolver接口
- 添加`toStandardType(Field field)`方法
- 移除默认实现，要求各连接器提供具体实现

### 2. 实现各连接器的toStandardType方法
- SQL Server：`SqlServerSchemaResolver`
- MySQL：`MySQLSchemaResolver`
- Oracle：`OracleSchemaResolver`
- PostgreSQL：`PostgreSQLSchemaResolver`

### 3. 修复TableGroupChecker
- 在`setFieldMapping`方法中使用`sourceSchemaResolver.toStandardType(s)`
- 避免直接复制源字段导致的类型污染

## 方案优势

1. **简单有效**：直接解决类型污染问题
2. **架构清晰**：利用现有的DataType映射机制
3. **维护简单**：保持现有架构不变
4. **扩展性强**：支持任意数据库类型组合

## 预期效果

1. **彻底解决类型不匹配错误**：SQL Server的`int identity`等数据库特定类型不再导致转换失败
2. **避免类型污染**：目标字段使用标准类型，所有数据库都能正确处理
3. **简化架构**：利用现有的DataType映射机制，保持架构清晰
4. **易于维护**：新增数据库类型时只需实现`toStandardType`方法