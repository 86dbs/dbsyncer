# 标准数据类型系统设计

## 设计原则

1. **按JDBC类型定义标准类型**：根据JDBC驱动返回的类型来定义标准类型，而不是数据库的原生类型。这是因为在数据同步过程中，我们通过JDBC接口获取数据，JDBC驱动会将数据库原生类型转换为JDBC类型。

2. **全面覆盖**：所有数据库Template实现中的convertToDatabaseType方法必须覆盖DataTypeEnum定义的全部类型，包括BYTE、SHORT等，确保类型转换的完整性与一致性。

3. **跨数据库兼容性**：确保在不同数据库间能够正确映射这些标准类型。

## 标准类型定义

### 结构化文本类型
- **JSON**：专门的结构化文本存储，支持特定的查询操作
  - MySQL: JDBC类型为VARCHAR，但通过特殊处理支持JSON操作
  - PostgreSQL: JDBC类型为JSON/JSONB
  - SQL Server: JDBC类型为NVARCHAR，通过应用层处理支持JSON操作
  
- **XML**：XML格式数据存储
  - MySQL: JDBC类型为LONGTEXT，通过应用层处理支持XML操作
  - PostgreSQL: JDBC类型为XML
  - SQL Server: JDBC类型为XML

### 普通字符串类型
- **STRING**：固定/可变长度的普通文本
  - MySQL: JDBC类型为CHAR/VARCHAR
  - PostgreSQL: JDBC类型为CHAR/VARCHAR
  - SQL Server: JDBC类型为CHAR/VARCHAR/NCHAR/NVARCHAR

### 大文本类型
- **TEXT**：专门优化的大容量文本存储
  - MySQL: JDBC类型为LONGTEXT
  - PostgreSQL: JDBC类型为TEXT
  - SQL Server: JDBC类型为TEXT/NTEXT

### 枚举和集合类型
- **ENUM**：枚举类型
  - MySQL: JDBC类型为VARCHAR，但有特殊的枚举值约束
  - PostgreSQL: JDBC类型为"user-defined"（用户定义类型）
  - 其他数据库: 映射到字符串或整数
  
- **SET**：集合类型
  - MySQL: JDBC类型为VARCHAR，但有特殊的集合值约束
  - 其他数据库: 映射到字符串或位图

### 数值类型
- **BYTE**: 8位整数，JDBC类型为TINYINT
- **SHORT**: 16位整数，JDBC类型为SMALLINT
- **INT**: 32位整数，JDBC类型为INTEGER
- **LONG**: 64位整数，JDBC类型为BIGINT
- **FLOAT**: 单精度浮点数，JDBC类型为REAL
- **DOUBLE**: 双精度浮点数，JDBC类型为DOUBLE
- **DECIMAL**: 精确小数，JDBC类型为DECIMAL

### 时间类型
- **DATE**: 日期类型，JDBC类型为DATE
- **TIME**: 时间类型，JDBC类型为TIME
- **TIMESTAMP**: 时间戳类型，JDBC类型为TIMESTAMP

### 二进制类型
- **BYTES**: 小容量二进制数据，JDBC类型为VARBINARY/BINARY
  - MySQL: JDBC类型为VARBINARY/BINARY
  - PostgreSQL: JDBC类型为BYTEA
  - SQL Server: JDBC类型为VARBINARY
  - Oracle: JDBC类型为RAW
  - SQLite: JDBC类型为BLOB
  
- **BLOB**: 大容量二进制数据，JDBC类型为BLOB
  - MySQL: JDBC类型为BLOB/MEDIUMBLOB/LONGBLOB（根据大小自动选择）
  - PostgreSQL: JDBC类型为BYTEA（PostgreSQL使用BYTEA存储所有二进制数据）
  - SQL Server: JDBC类型为VARBINARY(MAX)
  - Oracle: JDBC类型为BLOB
  - SQLite: JDBC类型为BLOB

**设计说明**：与字符串类型的设计保持一致，`BYTES` 对应 `STRING`（小容量），`BLOB` 对应 `TEXT`（大容量）。这样可以更精确地控制二进制数据类型的选择，简化类型转换逻辑。

### 其他类型
- **BOOLEAN**: 布尔类型，JDBC类型为BIT/BOOLEAN

## 实现策略

1. **以JDBC类型为准**：在设计DataType实现类时，应以JDBC驱动返回的类型为准，而不是数据库原生类型。

2. **特殊处理**：对于需要特殊处理的类型（如JSON、ENUM等），在DataType实现类中提供相应的转换逻辑。

3. **类型映射**：在SchemaResolver中正确映射JDBC类型到标准类型，确保类型转换的准确性。

4. **数据库特定处理**：在各数据库的Template中处理数据库特定的类型转换逻辑。