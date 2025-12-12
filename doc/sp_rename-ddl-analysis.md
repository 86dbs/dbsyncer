# SQL Server sp_rename DDL 处理问题分析文档

## 一、问题概述

### 1.1 问题描述

在 SQL Server CT 模式下，当检测到列重命名操作时，生成的 `sp_rename` DDL 无法被 JSQLParser 解析，导致 `NullPointerException`。

### 1.2 影响范围

- SQL Server CT 到 SQL Server CT 的 DDL 同步
- SQL Server CT 到其他数据库的 DDL 同步（异构场景）
- 字段映射更新机制

## 二、问题分析

### 2.1 sp_rename 无法被 JSQLParser 解析的问题

#### 2.1.1 现象

当 `compareTableSchema` 正确识别出 RENAME 操作时，会生成：
```sql
EXEC sp_rename 'dbo.ddlTestSource.first_name', 'full_name', 'COLUMN'
```

这个 DDL 被传递到 `DDLParserImpl.parse()` 方法，但 JSQLParser 无法解析存储过程调用，导致 `ddlOperationEnum` 为 `null`，`isDDLOperationAllowed()` 方法中抛出 `NullPointerException`。

#### 2.1.2 根本原因

1. **JSQLParser 限制**：JSQLParser 只能解析标准的 SQL DDL 语句（如 `ALTER TABLE`），无法解析存储过程调用（如 `EXEC sp_rename`）
2. **设计问题**：`DDLParserImpl` 是通用的 DDL 解析器，当前设计假设所有 DDL 都可以被 JSQLParser 解析

## 三、解决方案

### 3.1 已实施：修复列属性比较逻辑 ✅

**修复内容**：在 `isColumnEqualIgnoreName` 方法中忽略 `nullable` 的比较

**代码位置**：`SqlServerCTListener.isColumnEqualIgnoreName()` (line 684-689)

**修复后的逻辑**：
```java
/**
 * 判断两个列是否相等（忽略列名）
 * 比较：类型、长度、精度
 * 注意：忽略 nullable 和 DEFAULT 值的比较，因为：
 * 1. SQL Server 在执行 sp_rename 后，列的 nullable 属性可能发生变化
 * 2. 快照保存/加载时，nullable 可能为 null（JSON 序列化/反序列化问题）
 * 3. DEFAULT 值不影响 RENAME 识别
 */
private boolean isColumnEqualIgnoreName(Field col1, Field col2) {
    return Objects.equals(col1.getTypeName(), col2.getTypeName())
            && Objects.equals(col1.getColumnSize(), col2.getColumnSize())
            && Objects.equals(col1.getRatio(), col2.getRatio());
}
```

**修复效果**：
- ✅ `testRenameColumn_RenameOnly` 测试现在能够正确识别 RENAME 操作，生成 `sp_rename` DDL 而不是 DROP + ADD

### 3.2 待实施：处理 sp_rename 解析问题 ⏳

**问题**：`sp_rename` DDL 无法被 JSQLParser 解析，导致 `NullPointerException`

**可选方案**：
1. **方案A**：在 `DDLParserImpl` 中添加 `sp_rename` 特殊处理（快速修复）
   - 在 `parse()` 方法开始处检测 `sp_rename`
   - 解析 `sp_rename` 参数，提取旧列名和新列名
   - 设置 `ddlOperationEnum = ALTER_CHANGE`
   - 设置 `changedFieldNames` 映射
   - 生成目标 SQL（替换表名和 schema）

2. **方案B**：在 `DDLChangedEvent` 中携带元数据，跳过 parse（架构优化）
   - 扩展 `DDLChangedEvent`，携带操作类型和字段映射信息
   - 在 `SqlServerCTListener.compareTableSchema()` 中生成 DDL 时，设置操作类型和字段映射
   - 在 `GeneralBufferActuator.parseDDl()` 中检查是否有预填充的信息，如果有则跳过 parse

**建议**：采用方案A进行快速修复，确保 `testRenameColumn_RenameAndModifyType` 测试能够通过。

## 四、关键代码位置

1. **列属性比较**：`SqlServerCTListener.isColumnEqualIgnoreName()` (line 684-689) ✅ 已修复
2. **RENAME DDL 生成**：`SqlServerCTListener.generateRenameColumnDDL()` (line 751-760)
3. **DDL 解析**：`DDLParserImpl.parse()` (line 64-171) ⏳ 待修复
4. **DDL 执行**：`GeneralBufferActuator.parseDDl()` (line 290-360)
5. **操作类型检查**：`GeneralBufferActuator.isDDLOperationAllowed()` (line 370) ⏳ 需要添加 null 检查

## 五、总结

### 5.1 核心问题

1. **列属性比较不准确**：`isColumnEqualIgnoreName` 方法比较 `nullable` 属性时过于严格 ✅ **已修复**
2. **sp_rename 无法解析**：JSQLParser 无法解析存储过程调用，导致 `NullPointerException` ⏳ **待处理**

### 5.2 修复状态

- ✅ **已修复**：`isColumnEqualIgnoreName` 方法忽略 `nullable` 比较
- ✅ **已验证**：`testRenameColumn_RenameOnly` 测试现在能够正确识别 RENAME 操作
- ⏳ **待处理**：`sp_rename` 解析问题（`testRenameColumn_RenameAndModifyType` 仍然失败）

### 5.3 下一步行动

1. 在 `DDLParserImpl` 中添加 `sp_rename` 特殊处理
2. 在 `isDDLOperationAllowed()` 中添加 `null` 检查作为防御性措施
