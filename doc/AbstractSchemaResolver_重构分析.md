# AbstractSchemaResolver 继承者方法提炼分析报告

## 概述
分析 `AbstractSchemaResolver` 的所有继承者（5个），识别可提炼到抽象类的重复代码模式。

## 继承者列表
1. `OracleSchemaResolver`
2. `PostgreSQLSchemaResolver`
3. `SqlServerSchemaResolver`
4. `MySQLSchemaResolver`
5. `SQLiteSchemaResolver`

---

## 可提炼的方法

### 1. ✅ `toStandardType(Field field)` - **完全相同的实现**

**现状：**
- 所有5个子类都有完全相同的实现
- 唯一差异：异常消息中的数据库名称不同（"Oracle"、"PostgreSQL"、"SQL Server"、"MySQL"、"SQLite"）

**实现模式：**
```java
@Override
public Field toStandardType(Field field) {
    DataType dataType = getDataType(field);
    if (dataType != null) {
        return new Field(field.getName(),
                dataType.getType().name(),
                getStandardTypeCode(dataType.getType()),
                field.isPk(),
                field.getColumnSize(),
                field.getRatio());
    }
    throw new UnsupportedOperationException(
        String.format("Unsupported [数据库名] type: %s...", field.getTypeName()));
}
```

**提炼方案：**
- 在抽象类中实现 `toStandardType` 方法
- 添加抽象方法 `protected abstract String getDatabaseName()` 用于获取数据库名称
- 子类只需实现 `getDatabaseName()` 返回数据库名称字符串

**收益：** 减少 ~15 行重复代码 × 5 = 75 行代码

---

### 2. ✅ `getStandardTypeCode(DataTypeEnum dataTypeEnum)` - **完全相同的实现**

**现状：**
- 所有5个子类都有完全相同的实现
- 实现：`return dataTypeEnum.ordinal();`

**提炼方案：**
- 直接提炼到抽象类，作为 `protected` 方法

**收益：** 减少 ~5 行代码 × 5 = 25 行代码

---

### 3. ✅ `getTargetTypeCode(String targetTypeName)` - **完全相同的实现**

**现状：**
- 所有5个子类都有完全相同的实现
- 实现：`return 0;`（带注释说明暂时返回0）

**提炼方案：**
- 直接提炼到抽象类，作为 `protected` 方法
- 如果未来需要不同实现，可以改为抽象方法或提供默认实现

**收益：** 减少 ~5 行代码 × 5 = 25 行代码

---

### 4. ✅ `fromStandardType(Field standardField)` - **模式相同，可提炼**

**现状：**
- 所有5个子类的实现模式完全相同
- 差异：每个子类有自己的 `STANDARD_TO_TARGET_TYPE_MAP` 静态映射表
- 都调用 `getTargetTypeName()` 方法（私有方法，访问映射表）

**实现模式：**
```java
@Override
public Field fromStandardType(Field standardField) {
    String targetTypeName = getTargetTypeName(standardField.getTypeName());
    return new Field(standardField.getName(),
            targetTypeName,
            getTargetTypeCode(targetTypeName),
            standardField.isPk(),
            standardField.getColumnSize(),
            standardField.getRatio());
}
```

**提炼方案（方案A - 仅提炼方法）：**
- 在抽象类中实现 `fromStandardType` 方法
- 添加抽象方法 `protected abstract String getTargetTypeName(String standardTypeName)` 
- 子类需要实现该方法，内部访问自己的映射表

**提炼方案（方案B - 提炼映射表 + 方法）：** ⭐ **推荐**
- 在抽象类中定义 `protected final Map<String, String> standardToTargetTypeMap`
- 在构造函数中初始化映射表，调用抽象方法 `initStandardToTargetTypeMapping(Map<String, String> mapping)` 让子类填充
- 在抽象类中实现 `getTargetTypeName()` 方法，使用映射表
- 在抽象类中实现 `fromStandardType` 方法
- 子类只需在 `initStandardToTargetTypeMapping` 中填充映射表即可
- 注意：SQLite 的特殊处理（`toUpperCase()`）可通过钩子方法 `getDefaultTargetTypeName()` 处理

**收益：** 
- 方案A：减少 ~10 行代码 × 5 = 50 行代码
- 方案B：减少 ~10 行代码 × 5 + ~20 行映射表代码 × 5 = 150 行代码

---

### 5. ⚠️ `toStandardTypeFromDDL(Field field, ColDataType colDataType)` - **部分相同**

**现状：**
- 只有 `MySQLSchemaResolver` 和 `SqlServerSchemaResolver` 实现了此方法
- 两个实现完全相同
- `OracleSchemaResolver`、`PostgreSQLSchemaResolver`、`SQLiteSchemaResolver` 未实现（使用接口默认实现抛出异常）

**实现模式：**
```java
@Override
public Field toStandardTypeFromDDL(Field field, ColDataType colDataType) {
    DataType dataType = getDataType(field);
    if (dataType != null) {
        Field result = new Field(field.getName(),
                dataType.getType().name(),
                getStandardTypeCode(dataType.getType()),
                field.isPk(),
                field.getColumnSize(),
                field.getRatio());
        
        Field ddlField = dataType.handleDDLParameters(colDataType);
        result.setColumnSize(ddlField.getColumnSize());
        result.setRatio(ddlField.getRatio());
        
        return result;
    }
    throw new UnsupportedOperationException(...);
}
```

**提炼方案：**
- 在抽象类中实现 `toStandardTypeFromDDL` 方法
- 使用 `getDatabaseName()` 来生成异常消息
- 这样所有子类都可以使用，无需重复实现

**收益：** 减少 ~20 行代码 × 2 = 40 行代码（当前），如果未来其他数据库也需要实现，可避免更多重复

---

## 新增：STANDARD_TO_TARGET_TYPE_MAP 提炼方案

### ✅ `STANDARD_TO_TARGET_TYPE_MAP` - **强烈推荐提炼**

**现状：**
- 所有5个子类都有相同的映射表结构
- 都是 `private static final Map<String, String>`
- 都在静态初始化块中填充
- 通过私有方法 `getTargetTypeName()` 访问

**问题：**
- 静态变量不利于测试和扩展
- 每个子类都有重复的映射表定义和管理代码
- 映射表初始化逻辑分散

**提炼方案：**
```java
// 抽象类中
protected final Map<String, String> standardToTargetTypeMap = new ConcurrentHashMap<>();

public AbstractSchemaResolver() {
    standardToTargetTypeMap = new ConcurrentHashMap<>();
    initStandardToTargetTypeMapping(standardToTargetTypeMap);
    initDataTypeMapping(mapping);
    Assert.notEmpty(mapping, "At least one data type is required.");
}

protected abstract void initStandardToTargetTypeMapping(Map<String, String> mapping);

protected String getTargetTypeName(String standardTypeName) {
    return standardToTargetTypeMap.getOrDefault(standardTypeName, 
                                                 getDefaultTargetTypeName(standardTypeName));
}

protected String getDefaultTargetTypeName(String standardTypeName) {
    return standardTypeName; // 默认返回原值，SQLite 可重写为 toUpperCase()
}
```

**子类示例：**
```java
@Override
protected void initStandardToTargetTypeMapping(Map<String, String> mapping) {
    mapping.put("INT", "INTEGER");
    mapping.put("STRING", "VARCHAR");
    // ... 其他映射
}
```

**收益：**
- 减少 ~20 行映射表定义代码 × 5 = 100 行代码
- 统一映射表管理机制
- 改为实例变量，更符合面向对象设计
- 初始化逻辑更清晰，与 DataType 映射初始化一致

**注意事项：**
- SQLite 需要重写 `getDefaultTargetTypeName()` 返回 `standardTypeName.toUpperCase()`
- 映射表初始化在构造函数中完成，确保线程安全

---

## 总结

### 提炼优先级

| 方法 | 优先级 | 难度 | 收益 |
|------|--------|------|------|
| `getStandardTypeCode` | ⭐⭐⭐ | 低 | 25行代码 |
| `getTargetTypeCode` | ⭐⭐⭐ | 低 | 25行代码 |
| `toStandardType` | ⭐⭐⭐ | 低 | 75行代码 |
| `fromStandardType` | ⭐⭐ | 中 | 50行代码 |
| `toStandardTypeFromDDL` | ⭐⭐ | 低 | 40行代码（当前） |

### 需要添加的抽象方法

**方案A（仅提炼方法）：**
1. `protected abstract String getDatabaseName()` - 获取数据库名称（用于异常消息）
2. `protected abstract String getTargetTypeName(String standardTypeName)` - 将标准类型转换为目标数据库类型名称

**方案B（提炼映射表 + 方法）：** ⭐ **推荐**
1. `protected abstract String getDatabaseName()` - 获取数据库名称（用于异常消息）
2. `protected abstract void initStandardToTargetTypeMapping(Map<String, String> mapping)` - 初始化标准类型到目标数据库类型的映射表
3. `protected String getDefaultTargetTypeName(String standardTypeName)` - 当映射表中不存在时的默认值（可选，默认返回原值，SQLite 可重写为 `toUpperCase()`）

### 预期收益

**方案A：**
- **代码减少：** 约 215 行重复代码
- **可维护性提升：** 统一的方法实现，修改一处即可影响所有子类
- **一致性提升：** 确保所有子类的行为一致

**方案B（推荐）：**
- **代码减少：** 约 315 行重复代码（215 + 100行映射表相关代码）
- **可维护性提升：** 统一的方法实现和映射表管理机制
- **一致性提升：** 确保所有子类的行为一致
- **结构优化：** 映射表统一管理，便于维护和扩展
- **初始化统一：** 映射表初始化与 DataType 映射初始化在同一构造函数中，逻辑更清晰

---

## 实施建议

### 步骤1：提炼简单方法
1. 提炼 `getStandardTypeCode` 和 `getTargetTypeCode` 到抽象类

### 步骤2：提炼 toStandardType
1. 添加抽象方法 `getDatabaseName()`
2. 在抽象类中实现 `toStandardType`，使用 `getDatabaseName()` 生成异常消息
3. 从所有子类中删除 `toStandardType` 实现
4. 在每个子类中实现 `getDatabaseName()` 方法

### 步骤3：提炼 STANDARD_TO_TARGET_TYPE_MAP 和 fromStandardType（推荐方案B）
1. 在抽象类中定义 `protected final Map<String, String> standardToTargetTypeMap`
2. 在构造函数中初始化映射表：`standardToTargetTypeMap = new ConcurrentHashMap<>();`
3. 在构造函数中调用 `initStandardToTargetTypeMapping(standardToTargetTypeMap)` 让子类填充
4. 在抽象类中实现 `protected String getTargetTypeName(String standardTypeName)`，使用映射表
5. 在抽象类中实现 `fromStandardType` 方法
6. 从所有子类中删除 `STANDARD_TO_TARGET_TYPE_MAP` 静态变量和静态初始化块
7. 从所有子类中删除 `fromStandardType` 和私有 `getTargetTypeName` 方法
8. 在每个子类中实现 `initStandardToTargetTypeMapping` 方法，填充映射表
9. （可选）如果子类需要特殊的默认值处理（如 SQLite 的 `toUpperCase()`），重写 `getDefaultTargetTypeName` 方法

### 步骤4：提炼 toStandardTypeFromDDL（可选）
1. 在抽象类中实现 `toStandardTypeFromDDL`
2. 从 MySQL 和 SQL Server 子类中删除实现
3. 其他子类自动获得支持（如果未来需要）

---

## 注意事项

1. **向后兼容性：** 所有提炼都是向后兼容的，不会破坏现有功能
2. **测试：** 提炼后需要充分测试所有数据库连接器的类型转换功能
3. **映射表初始化顺序：** 如果采用方案B，映射表初始化需要在构造函数中完成，确保在 `fromStandardType` 调用前已初始化
4. **SQLite 特殊处理：** SQLite 的 `getTargetTypeName` 在 `getOrDefault` 时使用了 `toUpperCase()`，需要在抽象类中提供钩子方法支持
5. **映射表访问：** 映射表改为实例变量而非静态变量，更符合面向对象设计原则

