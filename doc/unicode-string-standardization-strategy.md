# Unicode 字符串类型标准化策略

## 问题背景

对于不区分 Unicode 和非 Unicode 的数据库（如 MySQL、PostgreSQL），它们的字符串类型（VARCHAR、TEXT）在标准化时应该转换成 `STRING` 还是 `UNICODE_STRING`？

## 数据库分类

### 1. 明确区分 Unicode 的数据库

- **SQL Server**: `VARCHAR` (非Unicode) vs `NVARCHAR` (Unicode)
- **Oracle**: `VARCHAR2` (非Unicode) vs `NVARCHAR2` (Unicode)

**处理策略**：
- Unicode 类型（NVARCHAR, NVARCHAR2） → `UNICODE_STRING`
- 非 Unicode 类型（VARCHAR, VARCHAR2） → `STRING`

### 2. 不区分但默认支持 Unicode 的数据库

- **MySQL**: `VARCHAR`/`CHAR`/`TEXT` 默认支持 UTF-8（通过字符集配置）
- **PostgreSQL**: `VARCHAR`/`CHAR`/`TEXT` 默认支持 UTF-8

**当前问题**：
- MySQL VARCHAR → `STRING` → 目标数据库可能选择非 Unicode 类型
- 如果源端存储的是 UTF-8 数据，转换到非 Unicode 类型可能丢失信息

## 推荐策略

### 方案：不区分的数据库默认标准化为 UNICODE_STRING

**理由**：
1. **数据安全**：MySQL/PostgreSQL 的字符串类型实际上都支持 UTF-8/Unicode
2. **避免信息丢失**：如果源端是 UTF-8 数据，标准化为 `UNICODE_STRING` 可以确保目标端选择 Unicode 类型
3. **向后兼容**：目标端映射 `UNICODE_STRING → VARCHAR` 在 MySQL/PostgreSQL 中仍然有效（因为 VARCHAR 支持 UTF-8）

**实现方式**：
- MySQL/PostgreSQL 的 StringType 实现类应该返回 `UNICODE_STRING` 而不是 `STRING`
- 或者创建 `MySQLUnicodeStringType` 继承 `UnicodeStringType`，但支持 VARCHAR/CHAR 类型

## 类型转换示例

### 场景1：MySQL → SQL Server
```
MySQL VARCHAR(50) [UTF-8数据]
  → UNICODE_STRING (标准化)
  → SQL Server NVARCHAR(50) ✅ 正确，支持 Unicode
```

### 场景2：MySQL → MySQL
```
MySQL VARCHAR(50) [UTF-8数据]
  → UNICODE_STRING (标准化)
  → MySQL VARCHAR(50) ✅ 正确，VARCHAR 支持 UTF-8
```

### 场景3：如果使用 STRING（当前方案，有问题）
```
MySQL VARCHAR(50) [UTF-8数据]
  → STRING (标准化)
  → SQL Server VARCHAR(50) ❌ 可能丢失 Unicode 字符
```

## 结论

**推荐**：对于不区分 Unicode 的数据库，默认标准化为 `UNICODE_STRING`，以确保数据安全性和兼容性。

