# 目标字段动态生成功能实施方案（统一字段值处理机制）

## 1. 需求分析

### 1.1 业务需求

在数据同步过程中，需要支持目标字段值通过界面配置的规则动态生成或转换。例如：
- **基于源字段计算生成新字段**（如：`full_name = first_name + ' ' + last_name`）- 目标表需要计算字段，源表只有基础字段
- **生成固定值字段**（如：`sync_batch_id`、`data_source`、`sync_time`）- 目标表需要记录同步相关的元数据
- **设置默认值**（如：`status` 字段如果为 null，设置为 "ACTIVE"）- 目标表字段有默认值要求
- **对已有字段值进行转换**（如：AES 加密、类型转换、格式转换）- 目标表需要对同步过来的值进行转换

### 1.2 现有实现分析

#### 1.2.1 字段映射机制

当前 `FieldMapping` 支持源字段和目标字段的映射，但 `Picker.exchange()` 方法要求源字段和目标字段都不为空才会进行映射：

```java
// dbsyncer-parser/src/main/java/org/dbsyncer/parser/model/Picker.java
if (null != sField && null != tField) {
    v = source.get(sField.getName());
    target.put(tFieldName, v);
}
```

**问题**：如果目标字段没有对应的源字段，该字段不会被填充到目标数据 Map 中。

#### 1.2.2 数据转换机制（Convert）

`ConvertUtil.convert()` 用于对**已有字段值**进行转换，执行时机在 `Picker.exchange()` **之后**：

```java
// 数据同步流程
// 1. 字段映射（Picker.exchange()）
targetDataList = picker.pickTargetData(...);

// 2. 参数转换（ConvertUtil.convert()）
ConvertUtil.convert(tableGroup.getConvert(), targetDataList);
```

**Convert 的特点**：
- ✅ 用于对**已有字段值**进行转换（字段必须通过字段映射从源表同步过来）
- ✅ 提供了丰富的 Handler（UUID、TIMESTAMP、DEFAULT、AES_ENCRYPT 等）
- ❌ **不能处理无源字段的目标字段**（字段不在 Map 中时，`row.get(name)` 返回 null，虽然某些 Handler 可以处理 null，但字段本身可能不在 `targetFields` 中）

### 1.3 设计思路

**统一字段值处理机制**：增强 Convert 机制，使其能够同时处理：
1. **字段值转换**：字段已有值（通过字段映射从源表同步），进行转换
2. **字段值生成**：字段无值（无源字段映射），生成新值

**核心优势**：
- ✅ 统一接口，用户不需要区分"转换"和"生成"
- ✅ 自动处理：字段有值就转换，无值就生成
- ✅ 复用现有 Convert Handler 机制
- ✅ 减少概念复杂度

## 2. 技术方案设计

### 2.1 方案概述

**增强 Convert 机制**，使其能够处理字段不在 Map 中的情况，统一处理字段值的生成和转换。

### 2.2 核心设计思路

1. **增强 ConvertUtil**：修改 `convert()` 方法，支持字段不在 Map 中的情况
2. **字段映射扩展**：自动将 Convert 配置中的字段添加到 FieldMapping 中（source 为 null）
3. **统一处理逻辑**：根据字段是否存在自动选择处理方式
4. **复用现有 Handler**：直接使用 ConvertEnum 中已定义的 Handler

### 2.3 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                     配置层（Configuration）                   │
├─────────────────────────────────────────────────────────────┤
│  Mapping (全局配置)                                          │
│    - convert: List<Convert>                                 │
│                                                              │
│  TableGroup (表级配置)                                       │
│    - convert: List<Convert>                                 │
│    - 优先使用 TableGroup 配置，为空则使用 Mapping 配置       │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                   合并层（Merge）                             │
├─────────────────────────────────────────────────────────────┤
│  PickerUtil.mergeTableGroupConfig()                         │
│    - 合并 convert 配置                                      │
│    - 自动添加到 FieldMapping（source=null, target=Field）   │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                   处理层（Processing）                        │
├─────────────────────────────────────────────────────────────┤
│  1. Picker.exchange()                                       │
│     - 支持 source=null 的情况（字段已在 FieldMapping 中）   │
│                                                              │
│  2. ConvertUtil.convert()（增强）                            │
│     - 字段在 Map 中：转换已有值                              │
│     - 字段不在 Map 中：生成新值（检查 targetFields）         │
└─────────────────────────────────────────────────────────────┘
```

### 2.4 数据同步流程

```
1. 字段映射（Picker.exchange()）
   - 有源字段映射：从源表同步值
   - 无源字段映射：字段不在 Map 中（但已在 FieldMapping 中）

2. 字段值处理（ConvertUtil.convert() - 增强）
   - 字段在 Map 中：转换已有值（如 AES 加密）
   - 字段不在 Map 中：生成新值（如 UUID、TIMESTAMP）

3. 插件处理
   - 插件可以进一步处理数据
```

## 3. 数据模型设计

### 3.1 Convert 模型（复用现有）

现有的 `Convert` 模型已经足够，无需修改：

```java
package org.dbsyncer.parser.model;

public class Convert {
    /**
     * 字段名称（目标字段）
     */
    private String name;
    
    /**
     * 转换名称
     * @see ConvertEnum
     */
    private String convertName;
    
    /**
     * 转换方式（ConvertEnum 的 code）
     * @see ConvertEnum
     */
    private String convertCode;
    
    /**
     * 转换参数
     */
    private String args;
    
    // Getters and Setters...
}
```

### 3.2 无需新增模型

**不需要**新增 `CustomField` 模型，直接使用现有的 `Convert` 模型。

## 4. 实现步骤

### 4.1 第一阶段：增强 ConvertUtil

#### 步骤 1.1：修改 ConvertUtil.convert() 方法

**文件**：`dbsyncer-parser/src/main/java/org/dbsyncer/parser/util/ConvertUtil.java`

**修改点**：直接修改现有的 `convert()` 方法，添加 `targetFields` 参数，支持字段不在 Map 中的情况

```java
/**
 * 转换参数（增强：支持字段值生成和转换）
 *
 * @param convert 转换配置列表
 * @param row 数据行
 * @param targetFields 目标字段列表（用于检查字段是否存在）
 */
public static void convert(List<Convert> convert, Map row, List<Field> targetFields) {
    if (CollectionUtils.isEmpty(convert) || row == null) {
        return;
    }
    
    final int size = convert.size();
    Convert c = null;
    String name = null;
    String code = null;
    String args = null;
    Object value = null;
    
    for (int i = 0; i < size; i++) {
        c = convert.get(i);
        name = c.getName();
        code = c.getConvertCode();
        args = c.getArgs();
        value = row.get(name);
        
        // 如果字段不在 Map 中，检查是否在 targetFields 中
        if (value == null && containsField(targetFields, name)) {
            // 字段在目标表中，但不在 Map 中（无源字段）
            // 使用 Handler 生成值（忽略 value 参数）
            value = ConvertEnum.getHandler(code).handle(args, null);
            row.put(name, value);
        } else if (value != null) {
            // 字段在 Map 中，进行转换
            value = ConvertEnum.getHandler(code).handle(args, value);
            row.put(name, value);
        }
        // 如果字段不在 Map 中且不在 targetFields 中，跳过（字段不存在）
    }
}

/**
 * 检查字段是否在目标字段列表中
 */
private static boolean containsField(List<Field> targetFields, String fieldName) {
    if (CollectionUtils.isEmpty(targetFields) || StringUtil.isBlank(fieldName)) {
        return false;
    }
    return targetFields.stream()
        .anyMatch(f -> f != null && StringUtil.equals(f.getName(), fieldName));
}
```

#### 步骤 1.2：修改 convert(List<Convert>, List<Map>) 方法

**文件**：`dbsyncer-parser/src/main/java/org/dbsyncer/parser/util/ConvertUtil.java`

**修改点**：修改批量处理方法，添加 `targetFields` 参数

```java
/**
 * 转换参数（批量处理）
 *
 * @param convert 转换配置列表
 * @param data 数据列表
 * @param targetFields 目标字段列表
 */
public static void convert(List<Convert> convert, List<Map> data, List<Field> targetFields) {
    if (!CollectionUtils.isEmpty(convert) && !CollectionUtils.isEmpty(data)) {
        data.forEach(row -> convert(convert, row, targetFields));
    }
}
```

### 4.2 第二阶段：字段映射扩展

#### 步骤 2.1：修改 PickerUtil.appendFieldMapping()

**文件**：`dbsyncer-parser/src/main/java/org/dbsyncer/parser/util/PickerUtil.java`

**修改点**：自动将 Convert 配置中的字段添加到 FieldMapping 中（source 为 null）

**重要说明**：
- `appendFieldMapping()` 在 `mergeTableGroupConfig()` 中调用，该方法在**运行时**（数据同步时）执行
- 该方法修改的是**运行时对象**，不会影响**持久化的配置**
- 编辑界面显示的是持久化的配置，因此**不会显示**这些自动添加的映射关系
- 如果 `source=null` 的映射关系出现在编辑界面，`[[${f?.source?.name}]]` 会显示为空，可能造成用户困惑

```java
private static void appendFieldMapping(Mapping mapping, TableGroup group) {
    final List<FieldMapping> fieldMapping = group.getFieldMapping();

    // 检查增量字段是否在映射关系中
    String eventFieldName = mapping.getListener().getEventFieldName();
    if (StringUtil.isNotBlank(eventFieldName)) {
        Map<String, Field> fields = convert2Map(group.getSourceTable().getColumn());
        addFieldMapping(fieldMapping, eventFieldName, fields, true);
    }

    // 检查过滤条件是否在映射关系中
    List<Filter> filter = group.getFilter();
    if (!CollectionUtils.isEmpty(filter)) {
        Map<String, Field> fields = convert2Map(group.getSourceTable().getColumn());
        filter.forEach(f -> addFieldMapping(fieldMapping, f.getName(), fields, true));
    }

    // 检查转换配置是否在映射关系中（增强：支持无源字段）
    List<Convert> convert = group.getConvert();
    if (!CollectionUtils.isEmpty(convert)) {
        Map<String, Field> targetFields = convert2Map(group.getTargetTable().getColumn());
        convert.forEach(c -> {
            String fieldName = c.getName();
            Field targetField = targetFields.get(fieldName);
            if (targetField != null) {
                // 检查是否已存在映射关系
                boolean exists = fieldMapping.stream()
                    .anyMatch(fm -> fm.getTarget() != null 
                        && StringUtil.equals(fm.getTarget().getName(), fieldName));
                if (!exists) {
                    // 添加映射关系（source 为 null，表示无源字段）
                    // 注意：这只是运行时添加，不会持久化到配置中，不会影响编辑界面
                    fieldMapping.add(new FieldMapping(null, targetField));
                }
            }
        });
    }
}
```

### 4.4 第四阶段：修改调用点

#### 步骤 3.1：修改 GeneralBufferActuator.distributeTableGroup()

**文件**：`dbsyncer-parser/src/main/java/org/dbsyncer/parser/flush/impl/GeneralBufferActuator.java`

**修改点**：修改 `convert()` 方法调用，传入 `targetFields` 参数

```java
// 2、参数转换（增强：支持字段值生成和转换）
TableGroup tableGroup = tableGroupPicker.getTableGroup();
List<Field> targetFields = tableGroupPicker.getTargetFields();
ConvertUtil.convert(tableGroup.getConvert(), targetDataList, targetFields);
```

**注意**：`targetDataList` 是 `List<Map>` 类型，需要调用 `convert(List<Convert>, List<Map>, List<Field>)` 方法。

#### 步骤 3.2：修改 ParserComponentImpl.processTableGroupDataBatch()

**文件**：`dbsyncer-parser/src/main/java/org/dbsyncer/parser/impl/ParserComponentImpl.java`

**修改点**：修改 `convert()` 方法调用，传入 `targetFields` 参数

```java
// 2、参数转换（增强：支持字段值生成和转换）
Picker picker = new Picker(tableGroup);
List<Field> targetFields = picker.getTargetFields();
ConvertUtil.convert(tableGroup.getConvert(), target, targetFields);
```

**注意**：`target` 是 `Map` 类型，需要调用 `convert(List<Convert>, Map, List<Field>)` 方法。

### 4.1 第一阶段：扩展 ConvertEnum 添加说明和示例

**文件**：`dbsyncer-parser/src/main/java/org/dbsyncer/parser/enums/ConvertEnum.java`

**修改点**：为每个转换类型添加 `description`（说明）和 `example`（示例）属性

```java
public enum ConvertEnum {

    /**
     * 默认值
     */
    DEFAULT("DEFAULT", "默认值", 1, 
        "字段值为 null 或空时使用默认值", 
        "参数：ACTIVE<br>原值：null → 结果：ACTIVE",
        new DefaultHandler()),
    
    /**
     * 系统时间戳
     */
    SYSTEM_TIMESTAMP("SYSTEM_TIMESTAMP", "系统时间戳", 0,
        "生成当前系统时间戳",
        "原值：任意 → 结果：2025-01-XX 10:00:00",
        new TimestampHandler()),
    
    /**
     * 系统日期Date
     */
    SYSTEM_DATE("SYSTEM_DATE", "系统日期", 0,
        "生成当前系统日期",
        "原值：任意 → 结果：2025-01-XX",
        new DateHandler()),
    
    /**
     * UUID
     */
    UUID("UUID", "UUID", 0,
        "生成 UUID 字符串",
        "原值：任意 → 结果：550e8400-e29b-41d4-a716-446655440000",
        new UUIDHandler()),
    
    /**
     * AES加密
     */
    AES_ENCRYPT("AES_ENCRYPT", "AES加密", 1,
        "使用 AES 算法加密字段值",
        "参数：encryption_key<br>原值：张三 → 结果：加密后的字符串",
        new AesEncryptHandler()),
    
    /**
     * AES解密
     */
    AES_DECRYPT("AES_DECRYPT", "AES解密", 1,
        "使用 AES 算法解密字段值",
        "参数：encryption_key<br>原值：加密字符串 → 结果：张三",
        new AesDecryptHandler()),
    
    /**
     * SHA1加密
     */
    SHA1("SHA1", "SHA1加密", 0,
        "使用 SHA1 算法加密字段值",
        "原值：password → 结果：5baa61e4c9b93f3f0682250b6cf8331b7ee68fd8",
        new Sha1Handler()),
    
    /**
     * 替换
     */
    REPLACE("REPLACE", "替换", 2,
        "替换字符串中的指定内容",
        "参数：old,new<br>原值：hello world → 结果：hello new",
        new ReplaceHandler()),
    
    /**
     * 前面追加
     */
    PREPEND("PREPEND", "前面追加", 1,
        "在字段值前面追加指定字符串",
        "参数：PREFIX_<br>原值：123 → 结果：PREFIX_123",
        new PrependHandler()),
    
    /**
     * 后面追加
     */
    APPEND("APPEND", "后面追加", 1,
        "在字段值后面追加指定字符串",
        "参数：_SUFFIX<br>原值：123 → 结果：123_SUFFIX",
        new AppendHandler()),
    
    // ... 其他转换类型类似，每个都添加 description 和 example
    
    // 转换编码
    private final String code;
    // 转换名称
    private final String name;
    // 参数个数
    private final int argNum;
    // 说明
    private final String description;
    // 示例（支持 HTML，使用 <br> 换行）
    private final String example;
    // 转换实现
    private final Handler handler;

    ConvertEnum(String code, String name, int argNum, String description, String example, Handler handler) {
        this.code = code;
        this.name = name;
        this.argNum = argNum;
        this.description = description;
        this.example = example;
        this.handler = handler;
    }
    
    // Getters
    public String getDescription() {
        return description;
    }
    
    public String getExample() {
        return example;
    }
    
    // ... 其他方法保持不变
}
```

**优势**：
- ✅ 说明和示例集中管理在 ConvertEnum 中
- ✅ 界面动态读取，不需要硬编码
- ✅ 修改说明和示例时，只需要修改 ConvertEnum，不需要修改界面
- ✅ 易于维护和扩展

**整合方案**：将表达式（EXPRESSION）和固定值（FIXED）也作为 ConvertEnum 中的特殊转换类型，统一管理所有转换类型和规则类型。

**修改 ConvertEnum**：在 ConvertEnum 中添加 EXPRESSION 和 FIXED 作为特殊转换类型

```java
public enum ConvertEnum {

    // ... 其他转换类型（DEFAULT、UUID、AES_ENCRYPT 等）...
    
    /**
     * 表达式规则
     */
    EXPRESSION("EXPRESSION", "表达式", -1,
        "使用表达式计算字段值，支持字段引用、字符串拼接、数学运算等",
        "表达式：${first_name} + ' ' + ${last_name}<br>结果：张 三",
        null),  // Handler 为 null，表示特殊类型
    
    /**
     * 固定值规则
     */
    FIXED("FIXED", "固定值", -1,
        "使用固定值作为字段值",
        "表达式：BATCH_001<br>结果：BATCH_001",
        null);  // Handler 为 null，表示特殊类型
    
    // ... 字段和方法保持不变 ...
    
    /**
     * 判断是否为特殊规则类型（不使用 Handler）
     */
    public boolean isSpecialRuleType() {
        return handler == null;
    }
}
```

**优势**：
- ✅ 所有转换类型和规则类型统一在 ConvertEnum 中管理
- ✅ 界面只需要读取一个枚举，简化实现
- ✅ 说明和示例统一管理，易于维护
- ✅ 不需要单独的 RuleTypeEnum，减少代码复杂度

### 4.3 第二阶段：增强 ConvertUtil

支持表达式规则（如 `${field1} + ' ' + ${field2}`），扩展 Convert 模型：

**文件**：`dbsyncer-parser/src/main/java/org/dbsyncer/parser/model/Convert.java`

**扩展 Convert 模型**：
```java
/**
 * 规则类型
 * - FUNCTION: 使用 ConvertEnum 的 Handler（默认）
 * - EXPRESSION: 表达式规则（如：${field1} + ' ' + ${field2}）
 * - FIXED: 固定值
 */
private String ruleType = "FUNCTION";  // 默认为 "FUNCTION"，表示使用 ConvertEnum

/**
 * 规则表达式（当 ruleType 为 EXPRESSION 时使用）
 * 支持语法：${fieldName} 表示字段值，支持字符串拼接、数学运算等
 * 示例：
 * - ${first_name} + ' ' + ${last_name}
 * - ${price} * 1.1
 * - 'PREFIX_' + ${id}
 */
private String ruleExpression;
```

#### 步骤 4.1：扩展 Convert 模型

**文件**：`dbsyncer-parser/src/main/java/org/dbsyncer/parser/model/Convert.java`

**添加字段**：
```java
/**
 * 规则类型
 */
private String ruleType = "FUNCTION";

/**
 * 规则表达式
 */
private String ruleExpression;

// Getters and Setters
public String getRuleType() {
    return ruleType;
}

public void setRuleType(String ruleType) {
    this.ruleType = ruleType;
}

public String getRuleExpression() {
    return ruleExpression;
}

public void setRuleExpression(String ruleExpression) {
    this.ruleExpression = ruleExpression;
}
```

#### 步骤 5.2：实现表达式解析器

**文件**：`dbsyncer-parser/src/main/java/org/dbsyncer/parser/util/ExpressionUtil.java`（新建）

**实现简单的表达式解析**：
```java
package org.dbsyncer.parser.util;

import org.dbsyncer.common.util.StringUtil;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ExpressionUtil {
    
    private static final Pattern FIELD_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");
    
    /**
     * 计算表达式
     * 
     * @param expression 表达式（如：${field1} + ' ' + ${field2}）
     * @param row 数据行
     * @return 计算结果
     */
    public static Object evaluate(String expression, Map<String, Object> row) {
        if (StringUtil.isBlank(expression)) {
            return null;
        }
        
        // 替换字段占位符 ${fieldName} 为实际值
        String result = expression;
        Matcher matcher = FIELD_PATTERN.matcher(expression);
        
        while (matcher.find()) {
            String fieldName = matcher.group(1);
            Object fieldValue = row.get(fieldName);
            String valueStr = fieldValue != null ? String.valueOf(fieldValue) : "";
            
            // 转义特殊字符，避免在字符串拼接时出错
            valueStr = escapeForExpression(valueStr);
            
            result = result.replace("${" + fieldName + "}", valueStr);
        }
        
        // 简单的表达式计算（支持字符串拼接和基本数学运算）
        return evaluateSimpleExpression(result);
    }
    
    /**
     * 转义特殊字符
     */
    private static String escapeForExpression(String value) {
        if (value == null) {
            return "";
        }
        // 如果值包含单引号，需要转义
        return value.replace("'", "''");
    }
    
    /**
     * 计算简单表达式（字符串拼接和基本数学运算）
     */
    private static Object evaluateSimpleExpression(String expression) {
        // 移除字符串引号，处理字符串拼接
        expression = expression.trim();
        
        // 如果表达式是纯字符串拼接（用 + 连接），直接拼接
        if (expression.contains("+") && !expression.matches(".*[0-9]+.*")) {
            // 字符串拼接
            String[] parts = expression.split("\\+");
            StringBuilder sb = new StringBuilder();
            for (String part : parts) {
                part = part.trim();
                // 移除单引号
                if (part.startsWith("'") && part.endsWith("'")) {
                    part = part.substring(1, part.length() - 1);
                }
                sb.append(part);
            }
            return sb.toString();
        }
        
        // 尝试数学运算（简单实现，仅支持基本运算）
        try {
            // 移除字符串引号，尝试计算
            expression = expression.replace("'", "");
            if (expression.matches(".*[0-9]+.*[+\\-*/].*[0-9]+.*")) {
                // 使用 JavaScript 引擎或简单的表达式计算
                return evaluateMathExpression(expression);
            }
        } catch (Exception e) {
            // 如果计算失败，返回原始表达式
        }
        
        // 默认返回处理后的字符串
        return expression.replace("'", "");
    }
    
    /**
     * 计算数学表达式（简单实现）
     */
    private static Object evaluateMathExpression(String expression) {
        // 这里可以使用 ScriptEngine 或其他表达式引擎
        // 简单实现：仅支持基本运算
        try {
            // 使用 ScriptEngineManager 计算表达式
            javax.script.ScriptEngineManager manager = new javax.script.ScriptEngineManager();
            javax.script.ScriptEngine engine = manager.getEngineByName("JavaScript");
            Object result = engine.eval(expression);
            return result;
        } catch (Exception e) {
            throw new RuntimeException("表达式计算失败: " + expression, e);
        }
    }
}
```

#### 步骤 5.3：修改 ConvertUtil 支持表达式

**文件**：`dbsyncer-parser/src/main/java/org/dbsyncer/parser/util/ConvertUtil.java`

**修改 `convert()` 方法**：
```java
public static void convert(List<Convert> convert, Map row, List<Field> targetFields) {
    if (CollectionUtils.isEmpty(convert) || row == null) {
        return;
    }
    
    final int size = convert.size();
    Convert c = null;
    String name = null;
    String code = null;
    String args = null;
    String ruleType = null;
    String ruleExpression = null;
    Object value = null;
    
    for (int i = 0; i < size; i++) {
        c = convert.get(i);
        name = c.getName();
        ruleType = c.getRuleType();
        ruleExpression = c.getRuleExpression();
        
        // 如果 ruleType 为 EXPRESSION，使用表达式计算
        if ("EXPRESSION".equals(ruleType) && StringUtil.isNotBlank(ruleExpression)) {
            value = ExpressionUtil.evaluate(ruleExpression, row);
            row.put(name, value);
            continue;
        }
        
        // 如果 ruleType 为 FIXED，使用固定值
        if ("FIXED".equals(ruleType) && StringUtil.isNotBlank(ruleExpression)) {
            value = ruleExpression;
            row.put(name, value);
            continue;
        }
        
        // 默认使用 ConvertEnum 的 Handler（FUNCTION 类型）
        code = c.getConvertCode();
        args = c.getArgs();
        value = row.get(name);
        
        // 如果字段不在 Map 中，检查是否在 targetFields 中
        if (value == null && containsField(targetFields, name)) {
            // 字段在目标表中，但不在 Map 中（无源字段）
            value = ConvertEnum.getHandler(code).handle(args, null);
            row.put(name, value);
        } else if (value != null) {
            // 字段在 Map 中，进行转换
            value = ConvertEnum.getHandler(code).handle(args, value);
            row.put(name, value);
        }
    }
}
```

## 5. 代码修改清单

### 5.1 修改文件

1. **`dbsyncer-parser/src/main/java/org/dbsyncer/parser/enums/ConvertEnum.java`**
   - 添加 `description` 字段（说明）
   - 添加 `example` 字段（示例）
   - 修改构造函数，支持说明和示例参数
   - 为所有转换类型添加说明和示例

2. **`dbsyncer-parser/src/main/java/org/dbsyncer/parser/model/Convert.java`**
   - 添加 `ruleType` 字段（规则类型：FUNCTION/EXPRESSION/FIXED）
   - 添加 `ruleExpression` 字段（规则表达式）

3. **`dbsyncer-parser/src/main/java/org/dbsyncer/parser/util/ExpressionUtil.java`**（新建）
   - 实现表达式解析和计算逻辑

4. **`dbsyncer-parser/src/main/java/org/dbsyncer/parser/util/ConvertUtil.java`**
   - 修改 `convert(List<Convert>, Map)` 方法，添加 `targetFields` 参数，支持表达式规则
   - 修改 `convert(List<Convert>, List<Map>)` 方法，添加 `targetFields` 参数

6. **`dbsyncer-parser/src/main/java/org/dbsyncer/parser/util/PickerUtil.java`**
   - 修改 `appendFieldMapping()`：自动将 Convert 配置中的字段添加到 FieldMapping 中

7. **`dbsyncer-parser/src/main/java/org/dbsyncer/parser/flush/impl/GeneralBufferActuator.java`**
   - 修改 `distributeTableGroup()`：调用增强版的 `convert()` 方法

8. **`dbsyncer-parser/src/main/java/org/dbsyncer/parser/impl/ParserComponentImpl.java`**
   - 修改 `processTableGroupDataBatch()`：调用增强版的 `convert()` 方法

**注意**：不需要修改 `BaseController.java`，因为 EXPRESSION 和 FIXED 已经整合到 ConvertEnum 中，界面可以直接从 `convert` 中读取。


## 6. 界面调整

### 6.1 Convert 配置界面说明

**文件**：`dbsyncer-web/src/main/resources/public/mapping/editConvert.html`

**界面布局**：
```
┌─────────────────────────────────────────────────────────────┐
│  转换配置                                                    │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┬──────────────────────┬─────────────────┐ │
│  │ 转换         │ 目标源表字段          │ 参数            │ │
│  ├──────────────┼──────────────────────┼─────────────────┤ │
│  │ [下拉选择]   │ [下拉选择]            │ [输入框] [输入框]│ │
│  │              │                      │                 │ │
│  │ - DEFAULT    │ - name (VARCHAR)      │                 │ │
│  │ - AES_ENCRYPT│ - status (VARCHAR)    │                 │ │
│  │ - UUID       │ - sync_batch_id (...) │                 │ │
│  │ - ...        │ - ...                 │                 │ │
│  └──────────────┴──────────────────────┴─────────────────┘ │
│                                                              │
│  [添加]                                                      │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 转换配置列表                                           │  │
│  ├──────────┬──────────────┬──────────┬───────────────┤  │
│  │ 转换      │ 目标源表字段 │ 参数     │ 操作           │  │
│  ├──────────┼──────────────┼──────────┼───────────────┤  │
│  │ AES_ENCRYPT│ name        │ key123   │ [删除]         │  │
│  │ DEFAULT    │ status      │ ACTIVE   │ [删除]         │  │
│  └──────────┴──────────────┴──────────┴───────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**操作流程**：

1. **选择转换类型**（第一个下拉框）：
   - 从 ConvertEnum 中选择转换类型（如：DEFAULT、AES_ENCRYPT、UUID 等）
   - 显示转换名称（如："默认值"、"AES加密"、"UUID"）

2. **选择目标字段**（第二个下拉框）：
   - 从目标表字段列表中选择字段
   - **关键点**：可以选择**任何目标表字段**，包括：
     - ✅ 有源字段映射的字段（如：`name`、`status`）
     - ✅ **无源字段映射的字段**（如：`sync_batch_id`、`data_source`）

3. **输入参数**（参数输入框）：
   - 根据转换类型输入参数（如：DEFAULT 需要默认值，AES_ENCRYPT 需要密钥）
   - 某些转换类型不需要参数（如：UUID、SYSTEM_TIMESTAMP）

4. **添加配置**：
   - 点击"添加"按钮，将配置添加到列表中
   - 系统会检查重复配置

5. **查看和管理**：
   - 在配置列表中查看已添加的转换规则
   - 可以删除不需要的配置

### 6.2 界面增强建议

**文件**：`dbsyncer-web/src/main/resources/public/mapping/editConvert.html`

**改进点**：

1. **添加提示信息**：
```html
<p class="text-muted">
    转换配置：为目标字段指定转换规则
    <br>
    <small class="text-info">
        • 字段有值：对已有字段值进行转换（如：AES 加密、类型转换）
        <br>
        • 字段无值：生成新字段值（如：固定值、默认值）
        <br>
        • 支持选择任何目标表字段，包括无源字段映射的字段
    </small>
</p>

<!-- 转换类型说明（可折叠，动态读取 ConvertEnum 的说明和示例） -->
<div class="panel panel-default" style="margin-top: 10px;">
    <div class="panel-heading">
        <h4 class="panel-title">
            <a data-toggle="collapse" href="#convertHelp">
                <i class="fa fa-question-circle"></i> 转换类型说明及示例
            </a>
        </h4>
    </div>
    <div id="convertHelp" class="panel-collapse collapse">
        <div class="panel-body">
            <div class="table-responsive">
                <table class="table table-bordered table-condensed">
                    <thead>
                        <tr>
                            <th style="width: 20%;">转换类型</th>
                            <th style="width: 15%;">参数个数</th>
                            <th style="width: 30%;">说明</th>
                            <th style="width: 35%;">示例</th>
                        </tr>
                    </thead>
                    <tbody>
                        <!-- 动态读取 ConvertEnum 的说明和示例（包括 EXPRESSION 和 FIXED） -->
                        <tr th:each="c : ${convert}">
                            <td><strong th:text="${c?.name}"></strong></td>
                            <td th:text="${c?.argNum == -1 ? '-' : c?.argNum}"></td>
                            <td th:text="${c?.description}"></td>
                            <td th:utext="${c?.example}"></td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>
</div>
```

2. **字段选择提示**（可选）：
```html
<select id="convertTargetField" class="form-control select-control">
    <option value="">-- 请选择目标字段 --</option>
    <!-- 目标源表公共字段 -->
    <option th:if="${tableGroup} == null" 
            th:each="c,s:${mapping?.targetColumn}" 
            th:value="${c?.name}" 
            th:text="${c?.name} +' (' + ${c?.typeName} +')'" />
    <!-- 目标源表字段 -->
    <option th:each="c,s:${tableGroup?.targetTable?.column}" 
            th:value="${c?.name}" 
            th:text="${c?.name} +' (' + ${c?.typeName} +')'" />
</select>
```

3. **转换类型说明**（可选）：
```html
<select id="convertOperator" class="form-control select-control">
    <option th:value="${c?.code}" 
            th:text="${c?.name}" 
            th:argNum="${c?.argNum}"
            th:title="${c?.name}：${c?.description}"
            th:each="c,state : ${convert}"/>
</select>
```

### 6.3 使用示例（界面操作）

**场景 1：为无源字段生成固定值**

1. 选择转换类型：`DEFAULT`（或未来扩展的 `FIXED`）
2. 选择目标字段：`sync_batch_id`（目标表有此字段，但源表没有）
3. 输入参数：`BATCH_001`
4. 点击"添加"
5. 结果：在配置列表中添加一条规则

**场景 2：为有源字段设置默认值**

1. 选择转换类型：`DEFAULT`
2. 选择目标字段：`status`（目标表有此字段，源表也有此字段）
3. 输入参数：`ACTIVE`
4. 点击"添加"
5. 结果：当 `status` 为 null 时，设置为 `ACTIVE`

**场景 3：为有源字段加密**

1. 选择转换类型：`AES_ENCRYPT`
2. 选择目标字段：`name`（目标表有此字段，源表也有此字段）
3. 输入参数：`encryption_key_123`
4. 点击"添加"
5. 结果：对 `name` 字段值进行 AES 加密

### 6.4 无需新增界面

**不需要**新增 `editCustomField.html`，直接使用现有的 `editConvert.html`。

**原因**：
- Convert 配置界面已经支持选择任何目标表字段
- 增强后的 ConvertUtil 会自动处理字段是否有值的情况
- 用户无需区分"转换"和"生成"，统一使用 Convert 配置

## 7. 使用示例

### 7.1 生成时间戳字段（无源字段）

**配置**：
- 字段映射：无（`create_time` 没有对应的源字段）
- 转换配置：`create_time` 使用 `SYSTEM_TIMESTAMP`

**流程**：
1. 字段映射：`create_time` 不在 Map 中（无源字段）
2. Convert 处理：检测到字段不在 Map 中，但在 `targetFields` 中，使用 `SYSTEM_TIMESTAMP` Handler 生成值
3. 结果：`create_time = "2025-01-XX 10:00:00"`

### 7.2 对已有字段值进行加密（有源字段）

**配置**：
- 字段映射：`源表.name → 目标表.name`
- 转换配置：`name` 使用 `AES_ENCRYPT`

**流程**：
1. 字段映射：`name = "张三"`（从源表同步）
2. Convert 处理：检测到字段在 Map 中，使用 `AES_ENCRYPT` Handler 转换值
3. 结果：`name = "加密后的值"`

### 7.3 有源字段默认值（字段值为 null）

**配置**：
- 字段映射：`源表.status → 目标表.status`（可能为 null）
- 转换配置：`status` 使用 `DEFAULT`，参数为 `"ACTIVE"`

**流程**：
1. 字段映射：`status = null` 或 `status = "INACTIVE"`（从源表同步，字段在 Map 中）
2. Convert 处理：
   - 如果 `status = null`：使用 `DEFAULT` Handler 生成默认值 `"ACTIVE"`
   - 如果 `status = "INACTIVE"`：保持原值（DEFAULT Handler 的逻辑）
3. 结果：`status = "ACTIVE"` 或 `status = "INACTIVE"`

**关键区别**：
- **无源字段**：字段不在 Map 中，需要生成新值
- **有源字段但值为 null**：字段在 Map 中但值为 null，使用 DEFAULT Handler 处理

## 8. 测试方案

### 8.1 单元测试

#### 测试用例 1：字段值生成（无源字段 - 固定值）
- **配置**：`sync_batch_id` 使用固定值 `"BATCH_001"`
- **字段映射**：无（`sync_batch_id` 没有对应的源字段）
- **预期结果**：`sync_batch_id` 字段值为 `"BATCH_001"`

#### 测试用例 2：字段值转换（有源字段）
- **配置**：`name` 使用 `AES_ENCRYPT`
- **字段映射**：`源表.name → 目标表.name`
- **预期结果**：`name` 字段值被加密

#### 测试用例 3：有源字段默认值（字段值为 null）
- **配置**：`status` 使用 `DEFAULT`，参数为 `"ACTIVE"`
- **字段映射**：`源表.status → 目标表.status`（可能为 null）
- **预期结果**：如果 `status = null`，则设置为 `"ACTIVE"`

### 8.2 集成测试

#### 测试场景 1：无源字段生成
1. 配置 Convert：`sync_batch_id` 使用固定值 `"BATCH_001"`（无源字段）
2. 执行全量同步
3. 验证目标表中 `sync_batch_id` 字段都有值且为 `"BATCH_001"`

#### 测试场景 2：有源字段转换
1. 配置字段映射：`源表.name → 目标表.name`
2. 配置 Convert：`name` 使用 `AES_ENCRYPT`
3. 执行同步
4. 验证目标表中 `name` 字段值被加密

#### 测试场景 3：有源字段默认值
1. 配置字段映射：`源表.status → 目标表.status`（可能为 null）
2. 配置 Convert：`status` 使用 `DEFAULT`，参数为 `"ACTIVE"`
3. 执行同步
4. 验证目标表中 `status` 字段：如果源表为 null，则设置为 `"ACTIVE"`

#### 测试场景 4：组合使用
1. 配置字段映射：`源表.name → 目标表.name`，`源表.status → 目标表.status`
2. 配置 Convert：
   - `sync_batch_id` 使用固定值 `"BATCH_001"`（无源字段）
   - `name` 使用 `AES_ENCRYPT`（有源字段）
   - `status` 使用 `DEFAULT`，参数为 `"ACTIVE"`（有源字段但可能为 null）
3. 执行同步
4. 验证所有字段都正确处理

## 9. 注意事项

### 9.1 方法签名变更

- `convert(List<Convert>, Map)` 方法签名变更为 `convert(List<Convert>, Map, List<Field>)`
- `convert(List<Convert>, List<Map>)` 方法签名变更为 `convert(List<Convert>, List<Map>, List<Field>)`
- 所有调用点需要传入 `targetFields` 参数
- 现有 Convert 配置继续有效，功能增强

### 9.2 字段检查

- 只有当字段在 `targetFields` 中时，才会生成值
- 如果字段不在目标表中，Convert 配置会被忽略

### 9.3 Handler 行为

- 某些 Handler（如 UUID、TIMESTAMP）会忽略 value 参数，直接生成新值
- 某些 Handler（如 DEFAULT）会检查 value 是否为 null，如果为 null 则使用 args
- 某些 Handler（如 AES_ENCRYPT）需要 value 不为 null

### 9.4 执行顺序

- Convert 在字段映射之后执行
- 如果字段在 Map 中，进行转换
- 如果字段不在 Map 中，生成新值


## 12. 表达式规则使用示例

### 12.1 表达式语法

**支持的语法**：
- `${fieldName}` - 字段值占位符
- `+` - 字符串拼接或数学加法
- `-` - 数学减法
- `*` - 数学乘法
- `/` - 数学除法
- `'text'` - 字符串字面量（单引号）

**示例**：
- `${first_name} + ' ' + ${last_name}` - 字符串拼接
- `${price} * 1.1` - 数学运算
- `'PREFIX_' + ${id}` - 固定前缀 + 字段值
- `${status} + '_' + ${version}` - 多字段拼接

### 12.2 使用场景

**场景 1：计算字段**
- 规则类型：`EXPRESSION`
- 表达式：`${first_name} + ' ' + ${last_name}`
- 结果：`"张 三"`

**场景 2：价格计算**
- 规则类型：`EXPRESSION`
- 表达式：`${price} * 1.1`
- 结果：价格 * 1.1（加10%）

**场景 3：固定值 + 字段值**
- 规则类型：`EXPRESSION`
- 表达式：`'BATCH_' + ${batch_id}`
- 结果：`"BATCH_001"`

**场景 4：固定值**
- 规则类型：`FIXED`
- 表达式：`BATCH_001`
- 结果：`"BATCH_001"`

---

**文档版本**：v2.0  
**创建日期**：2025-01-XX  
**最后更新**：2025-01-XX  
**更新说明**：
- v2.0：采用统一字段值处理机制，增强 Convert 机制，简化设计
