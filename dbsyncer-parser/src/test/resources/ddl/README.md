# DDL同步集成测试说明

## 测试概述

本目录包含DDL同步功能的集成测试资源，用于验证DBSyncer在不同数据库之间的DDL同步能力。

## 测试文件说明

### 测试数据文件
- `mysql-test-data.sql`: MySQL数据库的测试表结构和DDL语句
- `sqlserver-test-data.sql`: SQL Server数据库的测试表结构和DDL语句
- `heterogeneous-test-data.sql`: 异构数据库的测试表结构和DDL语句
- `test-table-structure.json`: 测试表结构的JSON描述

## 运行测试

### 环境准备
1. 确保已安装并启动MySQL和SQL Server数据库
2. 创建测试数据库
3. 在测试数据库中执行相应的SQL文件创建测试表

### 测试执行
```bash
# 在项目根目录执行Maven测试
mvn test -Dtest=org.dbsyncer.parser.ddl.DDLSyncIntegrationTest
mvn test -Dtest=org.dbsyncer.parser.ddl.MySQLToMySQLDDLSyncTest
mvn test -Dtest=org.dbsyncer.parser.ddl.SQLServerToSQLServerDDLSyncTest
mvn test -Dtest=org.dbsyncer.parser.ddl.HeterogeneousDDLSyncTest
mvn test -Dtest=org.dbsyncer.biz.impl.DDLSyncCoordinationTest
```

## 测试覆盖范围

### 数据库类型
- MySQL → MySQL
- SQL Server → SQL Server
- SQL Server → MySQL

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