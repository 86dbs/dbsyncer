# DBSyncer 字段映射类型标准化方案（优化版）

## 问题背景

在异构数据同步过程中，不同数据库的特定类型（如SQL Server的`int identity`、Oracle的`NUMBER(10,0)`等）在跨数据库同步时会出现类型不兼容的问题。

### 典型错误示例
```
org.dbsyncer.sdk.SdkException: MySQLSchemaResolver does not support type [class java.lang.Integer] convert to [int identity], val [501]
```

## 问题分析

### 根本原因
1. **字段映射配置问题**：在TableGroup的字段映射中，目标字段的类型仍然是数据库特定类型（如`int identity`）
2. **类型不匹配**：目标数据库的SchemaResolver无法处理源数据库的特定类型
3. **缺乏类型标准化机制**：没有统一的机制将数据库特定类型转换为标准类型

### 现有架构分析
DBSyncer采用中间类型转换模式：
```
源数据库特定类型 → 标准中间类型 → 目标数据库特定类型
```

当前SchemaResolver提供的方法：
- `merge(Object val, Field field)` - 将数据库特定类型转换为标准类型
- `convert(Object val, Field field)` - 将标准类型转换为目标数据库类型

### 关键发现
通过深入分析现有代码发现：
1. **大量的merge和convert方法实现相同**：大部分DataType类中，merge方法直接调用super.convert()
2. **"源→中间→目标"的两次Java类型转换没有必要**：都是Java类型转换，没有本质区别
3. **现有SDK架构已经很好地处理了类型转换**：AbstractDataType提供了良好的基础

## 解决方案设计

### 统一类型方案：极简架构设计

#### 设计理念
基于深入分析，采用**统一类型方案**，实现极简架构：

1. **字段映射统一使用标准类型**：源字段和目标字段都存储标准类型
2. **简化数据转换流程**：只需要一次convert转换，无需merge
3. **充分利用现有架构**：基于现有DataType映射机制进行反向查找

#### 第一部分：SchemaResolver接口简化

基于深入分析，简化SchemaResolver接口，合并merge和convert方法：

```java
public interface SchemaResolver {
    // 统一的数据转换方法：将数据转换为标准类型（合并merge和convert的功能）
    Object convert(Object val, Field field);
    
    // 新增字段类型标准化方法
    /**
     * 将数据库特定类型转换为标准类型
     * @param field 数据库特定类型字段
     * @return 标准化后的字段
     */
    Field toStandardType(Field field);
}
```

#### 复用现有Field类
DBSyncer现有的`Field`类已经包含了所有必要的类型信息：
- `typeName` - 类型名称（如 "int identity", "INT"）
- `type` - JDBC类型编码（如 4, -5）
- `columnSize` - 字段大小
- `ratio` - 小数位数（相当于scale）
- `pk` - 是否主键
- `name` - 字段名

因此无需创建新的类型信息类，直接使用`Field`类即可。

#### 第二部分：各数据库SchemaResolver实现改造

需要为每个数据库连接器实现`toStandardType`方法，利用现有的DataType映射机制：

```java
public class SqlServerSchemaResolver extends AbstractSchemaResolver {
    
    @Override
    public Field toStandardType(Field field) {
        // 利用现有的DataType映射机制进行类型转换
        DataType dataType = getDataType(field.getTypeName());
        if (dataType != null) {
            // 使用DataType的getType()方法获取标准类型
            return new Field(field.getName(), 
                           dataType.getType().name(), 
                           getStandardTypeCode(dataType.getType()),
                           field.isPk(), 
                           field.getColumnSize(), 
                           field.getRatio());
        }
        
        // 如果没有找到对应的DataType，抛出异常以发现系统不足
        throw new UnsupportedOperationException(
            String.format("Unsupported database type: %s. Please add mapping for this type in DataType configuration.", 
                         field.getTypeName()));
    }
}
```

**需要改造的连接器**：
- SQL Server连接器：`SqlServerSchemaResolver`
- MySQL连接器：`MySQLSchemaResolver`  
- Oracle连接器：`OracleSchemaResolver`
- 其他数据库连接器：按需实现

#### 第三部分：TableGroupChecker集成实现

核心改造：在 `getTable()` 方法中进行类型标准化，后续的字段映射创建过程会自动使用标准类型。

#### 第四部分：数据同步流程优化

在统一类型方案中，数据同步流程大大简化：

```java
// 在Picker中的exchange方法
private void exchange(int sFieldSize, int tFieldSize, List<Field> sFields, List<Field> tFields, Map<String, Object> source, Map<String, Object> target) {
    Field sField = null;
    Field tField = null;
    Object v = null;
    String tFieldName = null;
    for (int k = 0; k < sFieldSize; k++) {
        sField = sFields.get(k);
        if (k < tFieldSize) {
            tField = tFields.get(k);
        }
        if (null != sField && null != tField) {
            v = source.get(sField.getName());
            
            // 统一使用convert方法进行数据转换（一次转换）
            if (targetResolver != null) {
                v = targetResolver.convert(v, tField);  // 直接转换为目标类型
            }
            
            tFieldName = tField.getName();
            // 映射值
            if (!target.containsKey(tFieldName)) {
                target.put(tFieldName, v);
                continue;
            }
            // 合并值
            target.put(tFieldName, StringUtil.toString(target.get(tFieldName)) + StringUtil.toString(v));
        }
    }
}
```

## 完整工作流程

### 配置阶段
```java
// 1. 字段映射创建
FieldMapping fieldMapping = new FieldMapping();
fieldMapping.setSource(originalSourceField);  // "int identity" (SQL Server特定)
fieldMapping.setTarget(originalTargetField);  // "int" (MySQL特定)

// 2. 类型标准化
Field standardSource = sourceResolver.toStandardType(originalSourceField);  // "int identity" → "INT"
Field standardTarget = targetResolver.toStandardType(originalTargetField);  // "int" → "INT"

// 3. 更新字段映射
fieldMapping.setSource(standardSource);  // "INT" (标准类型)
fieldMapping.setTarget(standardTarget);  // "INT" (标准类型)
```

### 运行时阶段
```java
// 1. 读取源数据
Object sourceValue = sourceDatabase.read("ID");  // 返回: 501 (Integer)

// 2. 直接转换为目标类型（一次转换）
Object targetValue = targetResolver.convert(sourceValue, fieldMapping.getTarget());  // 501 (Integer) → 501 (Integer)

// 3. 写入目标数据库
targetDatabase.write("ID", targetValue);
```

## 方案优势

### 1. **架构极简**
- 字段映射只存储标准类型，逻辑清晰
- 消除了复杂的中间类型处理
- 减少了类型转换的中间步骤

### 2. **性能提升**
- 从两次Java类型转换减少到一次转换
- 简化了数据同步流程
- 减少了不必要的对象创建

### 3. **维护简单**
- 不需要维护复杂的类型转换链
- 新增数据库类型时，只需要实现`toStandardType`方法
- 充分利用现有的DataType映射机制

### 4. **扩展性强**
- 基于现有架构，易于扩展
- 支持任意数据库类型组合
- 向后兼容现有功能

### 5. **调试友好**
- 类型转换路径清晰
- 便于问题定位和调试
- 提供完整的类型转换信息

## 实施步骤

### 第一阶段：SchemaResolver接口简化
1. 在`SchemaResolver`接口中增加`toStandardType`方法
2. 为`AbstractSchemaResolver`提供默认实现
3. 逐步废弃`merge`方法，统一使用`convert`方法

### 第二阶段：各数据库连接器实现
1. **SQL Server连接器**：在`SqlServerSchemaResolver`中实现`toStandardType`方法
2. **MySQL连接器**：在`MySQLSchemaResolver`中实现`toStandardType`方法
3. **Oracle连接器**：在`OracleSchemaResolver`中实现`toStandardType`方法
4. **其他连接器**：为PostgreSQL、MongoDB等连接器实现相应方法

**实现要点**：
- 利用现有的`getDataType(field.getTypeName())`方法获取DataType
- 使用`dataType.getType().name()`获取标准类型名称
- 使用`getStandardTypeCode(dataType.getType())`获取标准类型编码
- 对于未支持的类型，抛出`UnsupportedOperationException`异常

### 第三阶段：UI端类型标准化（关键环节）
1. 修改`TableGroupChecker.getTable`方法，在UI加载时进行类型标准化
2. 更新UI显示逻辑，展示标准类型名称
3. 确保UI、存储、运行时都使用标准类型，避免数据不一致

### 第四阶段：TableGroupChecker集成
1. **修改getTable方法**：在UI加载时进行类型标准化（核心改造）
2. **集成SchemaResolver**：获取连接器的SchemaResolver进行类型转换
3. **验证流程**：确保后续的字段映射创建自动使用标准类型

### 第五阶段：数据同步流程优化
1. **修改Picker.exchange方法**：统一使用`targetResolver.convert`进行数据转换
2. **简化转换逻辑**：从两次转换（merge + convert）简化为一次转换
3. **优化性能**：减少不必要的类型转换操作

### 第六阶段：测试和验证
1. **单元测试**：验证各连接器的`toStandardType`方法
2. **集成测试**：验证端到端的类型转换流程
3. **兼容性测试**：测试各种数据库类型组合的转换
4. **向后兼容性验证**：确保现有配置不受影响
5. **性能测试**：验证类型转换的性能提升

## 预期效果

### 解决的问题
1. **类型不匹配错误**：彻底解决`int identity`等数据库特定类型的转换问题
2. **数据不一致问题**：UI显示、存储、运行时都使用标准类型，避免数据不一致
3. **用户体验问题**：用户看到统一的类型名称，更易理解和配置
4. **架构复杂性**：大大简化整个类型转换架构
5. **维护成本**：显著降低系统的维护成本
6. **性能问题**：提升数据同步性能

### 性能提升
1. **减少类型转换次数**：从两次转换减少到一次转换
2. **简化处理逻辑**：统一使用标准类型，减少条件判断
3. **提高可读性**：代码逻辑更清晰，便于理解和维护

### 扩展性增强
1. **新增数据库支持**：只需要实现`toStandardType`方法
2. **类型映射管理**：充分利用现有的DataType映射机制
3. **配置灵活性**：支持更灵活的类型转换配置

## UI端类型标准化详细方案

### 问题分析
通过深入分析发现，UI端在编辑字段映射时存在数据不一致问题：

#### 当前流程的问题：
1. **UI加载时**：显示的是原始数据库特定类型（如`int identity`、`NUMBER(10,0)`等）
2. **用户编辑时**：基于原始类型进行字段映射配置
3. **保存时**：字段映射中存储的是原始类型
4. **运行时**：出现类型不匹配错误

#### 数据不一致的根源：
- **UI显示**：原始类型
- **字段映射存储**：原始类型  
- **运行时处理**：期望标准类型

### 解决方案：UI端类型标准化

在UI加载时就进行类型标准化，确保数据一致性：

#### 1. 修改TableGroupChecker.getTable方法
```java
private Table getTable(String connectorId, String tableName, String primaryKeyStr) {
    MetaInfo metaInfo = parserComponent.getMetaInfo(connectorId, tableName);
    Assert.notNull(metaInfo, "无法获取连接器表信息:" + tableName);
    
    // 获取连接器的SchemaResolver进行类型标准化
    ConnectorConfig connectorConfig = getConnectorConfig(connectorId);
    ConnectorService connectorService = connectorFactory.getConnectorService(connectorConfig.getConnectorType());
    SchemaResolver schemaResolver = connectorService.getSchemaResolver();
    
    // 对字段进行类型标准化
    List<Field> standardizedFields = new ArrayList<>();
    for (Field field : metaInfo.getColumn()) {
        Field standardizedField = schemaResolver.toStandardType(field);
        standardizedFields.add(standardizedField);
    }
    
    // 自定义主键处理
    if (StringUtil.isNotBlank(primaryKeyStr) && !CollectionUtils.isEmpty(standardizedFields)) {
        String[] pks = StringUtil.split(primaryKeyStr, StringUtil.COMMA);
        Arrays.stream(pks).forEach(pk -> {
            for (Field field : standardizedFields) {
                if (StringUtil.equalsIgnoreCase(field.getName(), pk)) {
                    field.setPk(true);
                    break;
                }
            }
        });
    }
    
    return new Table(tableName, metaInfo.getTableType(), standardizedFields, metaInfo.getSql(), metaInfo.getIndexType());
}
```

#### 2. UI显示效果对比
```java
// 修改前：UI显示原始类型
源表字段: ID (int identity), Name (nvarchar), Age (int)
目标表字段: ID (int), Name (varchar), Age (int)

// 修改后：UI显示标准类型
源表字段: ID (INT), Name (VARCHAR), Age (INT)  
目标表字段: ID (INT), Name (VARCHAR), Age (INT)
```

#### 3. 字段映射配置对比
```java
// 修改前：存储原始类型
FieldMapping {
    source: Field("ID", "int identity", 4, true, 10, 0)
    target: Field("ID", "int", 4, true, 10, 0)
}

// 修改后：存储标准类型
FieldMapping {
    source: Field("ID", "INT", 4, true, 10, 0)
    target: Field("ID", "INT", 4, true, 10, 0)
}
```

### UI端类型标准化的优势

1. **数据一致性**：UI、存储、运行时都使用标准类型
2. **用户体验**：用户看到统一的类型名称，更易理解
3. **配置简化**：不需要了解数据库特定的类型名称
4. **错误预防**：从源头避免类型不匹配问题
5. **向后兼容**：现有配置可以逐步迁移

### 实施建议

1. **第一阶段**：修改`getTable`方法，在UI加载时进行类型标准化
2. **第二阶段**：更新UI显示逻辑，展示标准类型名称
3. **第三阶段**：提供配置迁移工具，将现有配置转换为标准类型
4. **第四阶段**：逐步废弃原始类型显示

## 向后兼容扩展方案

### 问题分析
为了简化历史配置迁移问题，可以采用扩展`Field`类的方式，保留原始类型信息的同时增加标准类型字段。

### 解决方案：扩展Field类

#### 1. 扩展Field类结构
```java
public class Field {
    // 现有字段保持不变（向后兼容）
    private String name;
    private String typeName;        // 保留原始类型名（如 "int identity"）
    private int type;              // 保留原始类型编码
    private boolean pk;
    private String labelName;
    private int columnSize;
    private int ratio;
    
    // 新增标准类型字段
    private String standardTypeName;  // 标准类型名（如 "INT"）
    private int standardType;         // 标准类型编码
    
    // 构造方法（自动计算标准类型）
    public Field(String name, String typeName, int type, boolean pk, int columnSize, int ratio) {
        this.name = name;
        this.typeName = typeName;
        this.type = type;
        this.pk = pk;
        this.columnSize = columnSize;
        this.ratio = ratio;
        // 自动计算标准类型
        this.standardTypeName = calculateStandardTypeName(typeName);
        this.standardType = calculateStandardType(typeName);
    }
    
    // 新增getter/setter
    public String getStandardTypeName() { return standardTypeName; }
    public void setStandardTypeName(String standardTypeName) { this.standardTypeName = standardTypeName; }
    public int getStandardType() { return standardType; }
    public void setStandardType(int standardType) { this.standardType = standardType; }
}
```

#### 2. 修改SchemaResolver接口
```java
public interface SchemaResolver {
    // 保留现有方法
    Object merge(Object val, Field field);
    Object convert(Object val, Field field);
    
    // 新增方法：计算标准类型
    default String calculateStandardTypeName(String originalTypeName) {
        // 利用现有的DataType映射机制
        DataType dataType = getDataType(originalTypeName);
        if (dataType != null) {
            return dataType.getType().name();
        }
        // 如果没有找到对应的DataType，抛出异常以发现系统不足
        throw new UnsupportedOperationException(
            String.format("Unsupported database type: %s. Please add mapping for this type in DataType configuration.", 
                         originalTypeName));
    }
    
    default int calculateStandardType(String originalTypeName) {
        DataType dataType = getDataType(originalTypeName);
        if (dataType != null) {
            return getStandardTypeCode(dataType.getType());
        }
        // 如果没有找到对应的DataType，抛出异常以发现系统不足
        throw new UnsupportedOperationException(
            String.format("Unsupported database type: %s. Please add mapping for this type in DataType configuration.", 
                         originalTypeName));
    }
}
```

#### 3. 修改TableGroupChecker.getTable方法
```java
private Table getTable(String connectorId, String tableName, String primaryKeyStr) {
    MetaInfo metaInfo = parserComponent.getMetaInfo(connectorId, tableName);
    Assert.notNull(metaInfo, "无法获取连接器表信息:" + tableName);
    
    // 获取连接器的SchemaResolver
    ConnectorConfig connectorConfig = getConnectorConfig(connectorId);
    ConnectorService connectorService = connectorFactory.getConnectorService(connectorConfig.getConnectorType());
    SchemaResolver schemaResolver = connectorService.getSchemaResolver();
    
    // 对字段进行标准类型计算（保留原始类型）
    List<Field> enhancedFields = new ArrayList<>();
    for (Field field : metaInfo.getColumn()) {
        // 创建增强的Field对象，自动计算标准类型
        Field enhancedField = new Field(field.getName(), field.getTypeName(), field.getType(),
                                      field.isPk(), field.getColumnSize(), field.getRatio());
        enhancedFields.add(enhancedField);
    }
    
    // 自定义主键处理
    if (StringUtil.isNotBlank(primaryKeyStr) && !CollectionUtils.isEmpty(enhancedFields)) {
        String[] pks = StringUtil.split(primaryKeyStr, StringUtil.COMMA);
        Arrays.stream(pks).forEach(pk -> {
            for (Field field : enhancedFields) {
                if (StringUtil.equalsIgnoreCase(field.getName(), pk)) {
                    field.setPk(true);
                    break;
                }
            }
        });
    }
    
    return new Table(tableName, metaInfo.getTableType(), enhancedFields, metaInfo.getSql(), metaInfo.getIndexType());
}
```

#### 4. UI显示策略
```java
// UI只显示标准类型（因为getTable已经进行了类型标准化）
public String getDisplayTypeName(Field field) {
    return field.getStandardTypeName();  // 只显示标准类型
}
```

#### 5. 运行时处理策略
```java
// 在Picker的exchange方法中
private void exchange(int sFieldSize, int tFieldSize, List<Field> sFields, List<Field> tFields, 
                     Map<String, Object> source, Map<String, Object> target) {
    for (int k = 0; k < sFieldSize; k++) {
        Field sField = sFields.get(k);
        Field tField = tFields.get(k);
        
        if (null != sField && null != tField) {
            Object v = source.get(sField.getName());
            
            // 优先使用标准类型进行转换
            if (sField.getStandardTypeName() != null && tField.getStandardTypeName() != null) {
                // 使用标准类型进行转换
                Field standardTargetField = new Field(tField.getName(), tField.getStandardTypeName(), 
                                                    tField.getStandardType(), tField.isPk(), 
                                                    tField.getColumnSize(), tField.getRatio());
                v = targetResolver.convert(v, standardTargetField);
            } else {
                // 降级到原始类型转换（向后兼容）
                v = targetResolver.convert(v, tField);
            }
            
            target.put(tField.getName(), v);
        }
    }
}
```

### 向后兼容方案的优势

1. **零破坏性**：现有配置完全不需要修改
2. **渐进式升级**：新配置自动获得标准类型支持
3. **调试友好**：可以同时看到原始类型和标准类型
4. **性能优化**：新配置享受标准类型转换的性能提升
5. **迁移简单**：不需要强制迁移，自然升级即可
6. **UI简洁**：只显示标准类型，用户体验更好
7. **系统完善性**：通过抛出异常主动发现类型映射的不足，推动系统完善

### 配置示例

#### 新配置（自动计算标准类型）
```json
{
  "source": {
    "name": "ID",
    "typeName": "int identity",      // 原始类型（保留）
    "type": 4,
    "standardTypeName": "INT",       // 自动计算的标准类型
    "standardType": 4,
    "pk": true,
    "columnSize": 10,
    "ratio": 0
  },
  "target": {
    "name": "ID", 
    "typeName": "int",               // 原始类型（保留）
    "type": 4,
    "standardTypeName": "INT",       // 自动计算的标准类型
    "standardType": 4,
    "pk": true,
    "columnSize": 10,
    "ratio": 0
  }
}
```

#### 老配置（向后兼容）
```json
{
  "source": {
    "name": "ID",
    "typeName": "int identity",      // 原始类型
    "type": 4,
    "standardTypeName": null,        // 老配置没有标准类型
    "standardType": 0,
    "pk": true,
    "columnSize": 10,
    "ratio": 0
  }
}
```

## 总结

本方案通过统一类型架构和UI端类型标准化，彻底解决了异构数据同步中的类型转换问题。通过将源类型和目标类型都标准化为通用类型，并在UI加载时就进行类型标准化，确保了数据的一致性，大大简化了系统架构，提升了性能和可维护性。

### 方案的核心价值

1. **彻底解决类型不匹配问题**：从UI显示到运行时处理，全程使用标准类型
2. **提升用户体验**：用户看到统一的类型名称，配置更简单直观
3. **简化系统架构**：统一类型处理，减少复杂的类型转换逻辑
4. **增强系统可靠性**：从源头避免数据不一致问题
5. **提供良好扩展性**：新增数据库支持只需实现`toStandardType`方法

### 关键创新点

1. **UI端类型标准化**：在UI加载时就进行类型标准化，确保数据一致性
2. **统一类型架构**：源类型和目标类型都使用标准类型表示
3. **一次转换原则**：运行时只进行一次类型转换，提升性能
4. **充分利用现有机制**：基于现有的DataType映射机制实现类型标准化
5. **向后兼容扩展**：通过扩展Field类保留原始类型信息，实现零破坏性升级

### 方案选择建议

#### 方案A：完全统一类型架构
- **适用场景**：新项目或可以接受配置迁移的项目
- **优势**：架构最简洁，性能最优
- **劣势**：需要迁移现有配置

#### 方案B：向后兼容扩展方案
- **适用场景**：生产环境，需要保持向后兼容
- **优势**：零破坏性，渐进式升级
- **劣势**：Field类结构稍复杂

**推荐**：对于DBSyncer这样的成熟产品，建议采用**方案B（向后兼容扩展方案）**，既能解决类型转换问题，又能保证现有用户的配置不受影响。

这个方案不仅解决了当前的`int identity`问题，还为未来支持更多数据库类型提供了良好的扩展性，是一个全面、系统性的解决方案。
