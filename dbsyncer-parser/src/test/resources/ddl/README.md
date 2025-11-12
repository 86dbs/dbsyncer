# DDL同步集成测试说明

## 测试概述

本目录包含DDL同步功能的集成测试资源，用于验证DBSyncer在不同数据库之间的DDL同步能力。

## 测试文件说明

### 测试数据文件
- `mysql-test-data.sql`: MySQL数据库的测试表结构和DDL语句
- `sqlserver-test-data.sql`: SQL Server数据库的测试表结构和DDL语句
- `heterogeneous-test-data.sql`: 异构数据库的测试表结构和DDL语句
- `test-table-structure.json`: 测试表结构的JSON描述
- `init-test-data.sql`: 测试环境初始化脚本
- `cleanup-test-data.sql`: 测试环境清理脚本

### 配置文件
- `test.properties`: 测试数据库连接配置文件

## 运行测试

### 环境准备
1. 确保已安装并启动MySQL和SQL Server数据库
2. 创建测试数据库
3. 在测试数据库中执行相应的SQL文件创建测试表（所有测试表名都以"ddlTest"为前缀，避免与业务表混淆）
4. 修改`test.properties`配置文件中的数据库连接信息

### 测试环境管理
测试框架会在测试开始前自动执行以下操作：
1. 读取`init-test-data.sql`脚本并在源数据库和目标数据库中创建测试表
2. 插入初始测试数据

测试结束后会自动执行以下操作：
1. 读取`cleanup-test-data.sql`脚本并清理测试表和数据
2. 确保测试环境的干净和可重复性

所有测试类都使用统一的[TestDatabaseManager](file://e:\github\dbsyncer\dbsyncer-parser\src\test\java\org\dbsyncer\parser\ddl\TestDatabaseManager.java#L32-L111)来管理测试环境，确保测试的一致性和可维护性。

### 配置文件说明
在`test.properties`文件中配置测试数据库连接信息：
```properties
# MySQL数据库配置
test.db.mysql.url=jdbc:mysql://127.0.0.1:3306/test_db
test.db.mysql.username=test_user
test.db.mysql.password=test_password
test.db.mysql.driver=com.mysql.cj.jdbc.Driver

# SQL Server数据库配置
test.db.sqlserver.url=jdbc:sqlserver://127.0.0.1:1433;DatabaseName=test_db;encrypt=false;trustServerCertificate=true
test.db.sqlserver.username=sa
test.db.sqlserver.password=test_password
test.db.sqlserver.driver=com.microsoft.sqlserver.jdbc.SQLServerDriver
```

### 测试执行
```bash
# 在项目根目录执行Maven测试
mvn test -Dtest=org.dbsyncer.parser.ddl.DDLMysqlTest
mvn test -Dtest=org.dbsyncer.parser.ddl.DDLSqlServerTest
mvn test -Dtest=org.dbsyncer.parser.ddl.HeterogeneousDDLSyncTest
```

## 测试覆盖范围

### 数据库类型
- MySQL → MySQL
- SQL Server → SQL Server
- SQL Server → MySQL
- MySQL → SQL Server

### DDL操作类型
- ALTER ADD (新增字段)
- ALTER DROP (删除字段)
- ALTER MODIFY (修改字段)
- ALTER CHANGE (重命名字段)

### 功能验证点
- DDL解析准确性
- 字段映射动态更新
- 防循环机制
- 异常处理
- 端到端同步流程
- 异构数据库数据类型映射
- 语法差异处理
- 约束处理
- 测试环境自动管理

## 测试设计原则

### 统一测试环境管理
所有测试类都使用[TestDatabaseManager](file://e:\github\dbsyncer\dbsyncer-parser\src\test\java\org\dbsyncer\parser\ddl\TestDatabaseManager.java#L32-L111)来管理测试环境，确保：
- 测试前自动创建所需表结构和初始数据
- 测试后自动清理测试数据，保持环境干净
- 跨数据库类型的一致性管理

### 表名隔离
所有测试表都使用"ddlTest"前缀，避免与业务表冲突，确保测试安全性。

### 统一断言风格
所有测试类都使用`assert`关键字进行断言，保持代码风格一致性。

### 完整的生命周期管理
使用`@BeforeClass`和`@AfterClass`注解确保测试环境的完整生命周期管理。