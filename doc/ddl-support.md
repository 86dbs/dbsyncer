# DDL 同步支持情况

本文档说明 DBSyncer 在不同数据库和场景下对 DDL 同步的支持情况，特别是对字段约束（NOT NULL、DEFAULT、COMMENT）的保留情况。

## 数据库覆盖情况

| 数据库 | ADD COLUMN | MODIFY COLUMN | CHANGE/RENAME | 元数据读取 |
|--------|------------|---------------|---------------|------------|
| MySQL | ✅ 完整支持 | ✅ 完整支持 | ✅ 完整支持 | ✅ 已修复 |
| SQL Server | ✅ NOT NULL+DEFAULT | ✅ NOT NULL | N/A | ✅ 已修复 |
| PostgreSQL | ✅ NOT NULL+DEFAULT | ⚠️ 仅类型 | N/A | ✅ 已修复 |
| Oracle | ✅ NOT NULL+DEFAULT | ✅ NOT NULL+DEFAULT | N/A | ✅ 已修复 |
| SQLite | ✅ NOT NULL+DEFAULT | ❌ 不支持 | ❌ 不支持 | ✅ 已修复 |

### 说明

- **✅ 完整支持**：支持 NOT NULL、DEFAULT、COMMENT 三种约束
- **✅ NOT NULL+DEFAULT**：支持 NOT NULL 和 DEFAULT，不支持 COMMENT（需要单独语句）
- **✅ NOT NULL**：仅支持 NOT NULL 约束
- **⚠️ 仅类型**：仅支持类型修改，约束需要单独语句
- **❌ 不支持**：数据库本身不支持该操作
- **N/A**：不适用（该数据库不支持该操作类型）

## 已修复的场景

### 1. DDL 同步场景（ADD COLUMN）

所有数据库的 `buildAddColumnSql` 方法已支持：
- ✅ NOT NULL 约束
- ✅ DEFAULT 默认值
- ✅ MySQL 额外支持 COMMENT 注释

**示例：**
```sql
-- MySQL
ALTER TABLE `test` ADD COLUMN `age` INT(11) NOT NULL DEFAULT 0 COMMENT '年龄'

-- SQL Server
ALTER TABLE [test] ADD [age] INT NOT NULL DEFAULT 0

-- PostgreSQL
ALTER TABLE "test" ADD COLUMN "age" INTEGER NOT NULL DEFAULT 0

-- Oracle
ALTER TABLE "test" ADD ("age" NUMBER(11) NOT NULL DEFAULT 0)
```

### 2. DDL 修改场景（MODIFY COLUMN）

不同数据库的支持情况：

- **MySQL**：完整支持 NOT NULL、DEFAULT、COMMENT
- **SQL Server**：支持 NOT NULL（DEFAULT 需要单独处理）
- **Oracle**：支持 NOT NULL、DEFAULT
- **PostgreSQL**：仅支持类型修改（约束需要单独语句）
- **SQLite**：不支持直接修改列

**示例：**
```sql
-- MySQL
ALTER TABLE `test` MODIFY COLUMN `age` INT(11) NOT NULL DEFAULT 0 COMMENT '年龄'

-- SQL Server
ALTER TABLE [test] ALTER COLUMN [age] INT NOT NULL

-- Oracle
ALTER TABLE "test" MODIFY ("age" NUMBER(11) NOT NULL DEFAULT 0)
```

### 3. DDL 重命名场景（CHANGE/RENAME COLUMN）

- **MySQL** 的 `buildRenameColumnSql`（CHANGE 操作）：完整支持 NOT NULL、DEFAULT、COMMENT
- 其他数据库的 RENAME 操作：仅重命名，不涉及约束修改

**示例：**
```sql
-- MySQL CHANGE 操作
ALTER TABLE `test` CHANGE COLUMN `old_name` `new_name` INT(11) NOT NULL DEFAULT 0 COMMENT '新名称'
```

### 4. 从数据库元数据读取字段信息

在 `AbstractDatabaseConnector.getMetaInfo` 方法中，已修复从数据库元数据读取字段信息时的遗漏：

- ✅ `nullable`：从 `NULLABLE` 列读取（`columnNoNulls` → `false`，`columnNullable` → `true`）
- ✅ `defaultValue`：从 `COLUMN_DEF` 列读取
- ✅ `comment`：从 `REMARKS` 列读取

## 注意事项

### 1. PostgreSQL 的 MODIFY 操作

PostgreSQL 的 `ALTER COLUMN TYPE` 和设置约束需要分开执行。当前实现仅处理类型修改，如果需要修改约束，需要生成多个 ALTER TABLE 语句。

**建议的改进：**
```sql
-- 类型修改
ALTER TABLE "test" ALTER COLUMN "age" TYPE INTEGER;

-- 约束修改（需要单独语句）
ALTER TABLE "test" ALTER COLUMN "age" SET NOT NULL;
ALTER TABLE "test" ALTER COLUMN "age" SET DEFAULT 0;
```

### 2. SQL Server 的 DEFAULT 值

SQL Server 的 `ALTER COLUMN` 不支持在同一个语句中修改 DEFAULT 值。如果需要修改 DEFAULT，需要使用单独的 `ALTER TABLE ... ADD CONSTRAINT` 语句。

**建议的改进：**
```sql
-- 列修改
ALTER TABLE [test] ALTER COLUMN [age] INT NOT NULL;

-- DEFAULT 约束（需要单独处理）
ALTER TABLE [test] ADD CONSTRAINT DF_test_age DEFAULT 0 FOR [age];
```

### 3. COMMENT 支持

不同数据库对注释的支持方式不同：

- **MySQL**：在 ADD/MODIFY/CHANGE 语句中直接支持 `COMMENT '注释'`
- **PostgreSQL**：需要使用 `COMMENT ON COLUMN table.column IS '注释';`
- **Oracle**：需要使用 `COMMENT ON COLUMN table.column IS '注释';`
- **SQL Server**：需要使用扩展属性 `EXEC sp_addextendedproperty 'MS_Description', '注释', 'SCHEMA', 'dbo', 'TABLE', 'table', 'COLUMN', 'column';`
- **SQLite**：不支持列注释

当前实现：
- ✅ MySQL：完整支持
- ⚠️ 其他数据库：暂未实现（需要单独处理）

### 4. 同构数据库场景

对于同构数据库（源数据库和目标数据库类型相同），DDL 同步使用 `alter.toString()` 直接生成 SQL。JSQLParser 应该能够保留原始约束信息，但建议进行测试验证。

## 建议的后续改进

### 1. PostgreSQL MODIFY 完整支持

为 PostgreSQL 的 MODIFY 操作生成多个 ALTER TABLE 语句，以完整支持约束修改：

```java
// 伪代码示例
if (field.getNullable() != null && !field.getNullable()) {
    sql.append("; ALTER TABLE ").append(tableName)
       .append(" ALTER COLUMN ").append(columnName)
       .append(" SET NOT NULL");
}
if (field.getDefaultValue() != null) {
    sql.append("; ALTER TABLE ").append(tableName)
       .append(" ALTER COLUMN ").append(columnName)
       .append(" SET DEFAULT ").append(field.getDefaultValue());
}
```

### 2. SQL Server DEFAULT 约束处理

在修改列时，如果包含 DEFAULT 值，生成额外的 DEFAULT 约束语句。

### 3. COMMENT 语句生成

为 PostgreSQL、Oracle、SQL Server 添加 COMMENT 语句生成功能，在 ADD/MODIFY 操作后自动添加注释。

### 4. 测试覆盖

建议在不同数据库和场景下进行完整测试：

- ✅ MySQL → MySQL（同构）
- ✅ MySQL → SQL Server（异构）
- ✅ SQL Server → MySQL（异构）
- ✅ PostgreSQL → PostgreSQL（同构）
- ✅ Oracle → Oracle（同构）
- ⚠️ 各种 MODIFY 操作场景
- ⚠️ CHANGE/RENAME 操作场景

## 相关文件

- `dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/model/Field.java` - Field 类定义（包含 nullable、defaultValue、comment 属性）
- `dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/parser/ddl/converter/AbstractSourceToIRConverter.java` - DDL 解析和转换
- `dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/sql/impl/*Template.java` - 各数据库的 SQL 模板实现
- `dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/connector/database/AbstractDatabaseConnector.java` - 数据库连接器基类（元数据读取）

## 更新历史

- **2024-XX-XX**：初始版本，修复 DDL 同步过程中信息遗漏问题
  - 添加 Field 类的 nullable、defaultValue、comment 属性
  - 修复所有数据库的 buildAddColumnSql 方法
  - 修复 MySQL、SQL Server、Oracle 的 buildModifyColumnSql 方法
  - 修复 MySQL 的 buildRenameColumnSql 方法
  - 修复从数据库元数据读取字段信息时的遗漏

