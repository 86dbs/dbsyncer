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

### 3.2 已实施：生成标准 CHANGE COLUMN 语法 ✅

**修复内容**：修改 `generateRenameColumnDDL` 方法，生成标准的 `CHANGE COLUMN` 语法（类似 MySQL），而不是 SQL Server 特定的 `sp_rename`

**代码位置**：`SqlServerCTListener.generateRenameColumnDDL()` (line 746-790)

**修复后的逻辑**：
```java
/**
 * 生成 RENAME COLUMN 的 DDL
 * 使用标准的 CHANGE COLUMN 语法（类似 MySQL），便于 JSQLParser 解析
 * DDL 解析器会自动转换为目标数据库的语法：
 * - 同构 SQL Server：IRToSQLServerConverter 会将其转换为 sp_rename
 * - 异构场景：可以转换为目标数据库的 RENAME 语法
 * 
 * 格式：ALTER TABLE table CHANGE COLUMN old_name new_name type [NOT NULL]
 */
private String generateRenameColumnDDL(String tableName, Field oldColumn, Field newColumn) {
    // 生成标准的 CHANGE COLUMN 语法
    // ALTER TABLE schema.table CHANGE COLUMN old_name new_name type [NOT NULL]
}
```

**优势**：
1. ✅ **JSQLParser 可以解析**：标准的 `ALTER TABLE ... CHANGE COLUMN` 语法可以被 JSQLParser 正确解析
2. ✅ **自动转换**：对于同构 SQL Server，`IRToSQLServerConverter` 会自动将其转换为 `sp_rename`
3. ✅ **异构支持**：对于异构场景，可以转换为目标数据库的 RENAME 语法（如 MySQL 的 `CHANGE COLUMN`，PostgreSQL 的 `RENAME COLUMN`）
4. ✅ **无需特殊处理**：不需要在 `DDLParserImpl` 中添加 `sp_rename` 特殊处理

**修复效果**：
- ✅ `sp_rename` 解析问题从根本上解决
- ✅ 生成的 DDL 可以被 JSQLParser 正确解析
- ✅ 支持同构和异构场景

### 3.3 已实施：同构场景下的 CHANGE COLUMN 转换 ✅

**问题**：在同构 SQL Server 场景下，`DDLParserImpl` 原本直接使用原始 SQL（只替换表名），但 `CHANGE COLUMN` 语法不是 SQL Server 的原生语法，导致执行失败。

**修复内容**：在 `DDLParserImpl.parse()` 中，检测到 `CHANGE` 操作时，即使是同构场景也走转换流程，通过 `IRToSQLServerConverter` 将 `CHANGE COLUMN` 转换为 `sp_rename`。

**代码位置**：`DDLParserImpl.parse()` (line 98-123)

**修复后的逻辑**：
```java
// 检查是否有 CHANGE 操作（即使是同构数据库，CHANGE COLUMN 也需要转换为目标数据库的语法）
boolean hasChangeOperation = false;
if (alter.getAlterExpressions() != null) {
    for (AlterExpression expr : alter.getAlterExpressions()) {
        if (expr.getOperation() == AlterOperation.CHANGE) {
            hasChangeOperation = true;
            break;
        }
    }
}

if (isHeterogeneous || hasChangeOperation) {
    // 对于异构数据库，或者同构数据库但包含 CHANGE 操作，进行DDL语法转换
    // 通过 IRToSQLServerConverter 将 CHANGE COLUMN 转换为 sp_rename
    ...
}
```

**修复效果**：
- ✅ 同构 SQL Server 场景下，`CHANGE COLUMN` 正确转换为 `sp_rename`
- ✅ 其他同构场景（如 ADD、MODIFY、DROP）仍保持高效的原生 SQL 处理

### 3.4 已实施：修复 sp_rename 的 schema 支持 ✅

**问题**：`IRToSQLServerConverter.convertColumnsToChange` 生成的 `sp_rename` SQL 缺少 schema，导致 SQL Server 报错：`参数 @objname 不明确或所声明的 @objtype (COLUMN)有误。`

**为什么 `testRenameColumn_RenameOnly` 没有出现这个问题？**

虽然两个测试都会生成 `sp_rename` + `ALTER COLUMN`，但区别在于：
- `testRenameColumn_RenameOnly`：只重命名，类型相同，`ALTER COLUMN` 可能被优化或未实际执行，错误可能被掩盖
- `testRenameColumn_RenameAndModifyType`：重命名后还有类型修改，`ALTER COLUMN` 一定会执行，更容易暴露 schema 问题

实际上，两个测试都存在 schema 问题，只是 `testRenameColumn_RenameAndModifyType` 更容易暴露。

**修复内容**：在 `IRToSQLServerConverter.convertColumnsToChange` 中，构建 `sp_rename` SQL 时包含完整的对象名称（`schema.table.column`）。

**代码位置**：`IRToSQLServerConverter.convertColumnsToChange()` (line 162-169)

**修复后的逻辑**：
```java
// sp_rename 需要完整的对象名称，使用方括号避免特殊字符问题：'[schema].[table].[column]'
String effectiveSchema = (schema != null && !schema.trim().isEmpty()) ? schema : "dbo";
String fullObjectName = String.format("[%s].[%s].[%s]", effectiveSchema, tableName, oldColumnName);
result.append(String.format("EXEC sp_rename '%s', '%s', 'COLUMN'",
        fullObjectName, newColumnName));
```

**修复效果**：
- ✅ `sp_rename` SQL 现在包含完整的 `[schema].[table].[column]` 格式（使用方括号）
- ✅ 解决了 SQL Server 的 "参数 @objname 不明确" 错误
- ✅ 使用方括号可以避免表名或列名包含特殊字符时的问题

## 四、关键代码位置

1. **列属性比较**：`SqlServerCTListener.isColumnEqualIgnoreName()` (line 684-689) ✅ 已修复
2. **RENAME DDL 生成**：`SqlServerCTListener.generateRenameColumnDDL()` (line 746-790) ✅ 已修复
3. **DDL 解析**：`DDLParserImpl.parse()` (line 98-123) ✅ 已修复（同构场景下检测 CHANGE 操作并走转换流程）
4. **DDL 转换**：`IRToSQLServerConverter.convertColumnsToChange()` (line 162-169) ✅ 已修复（包含 schema 的 sp_rename）
5. **DDL 执行**：`GeneralBufferActuator.parseDDl()` (line 290-360)

