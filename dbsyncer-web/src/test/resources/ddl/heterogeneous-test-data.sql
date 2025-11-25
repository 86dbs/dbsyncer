-- 异构数据库测试数据

-- SQL Server源表
CREATE TABLE ddlTestEmployee (
    id INT IDENTITY(1,1) PRIMARY KEY,
    first_name NVARCHAR(50) NOT NULL,
    last_name NVARCHAR(50),
    department NVARCHAR(100),
    created_at DATETIME2 DEFAULT GETDATE()
);

-- MySQL目标表 (从SQL Server同步)
CREATE TABLE ddlTestEmployee (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50),
    department VARCHAR(100),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- SQL Server到MySQL的DDL测试语句

-- SQL Server到MySQL的ADD COLUMN
ALTER TABLE ddlTestEmployee ADD salary DECIMAL(10,2);

-- SQL Server到MySQL的ALTER COLUMN
ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(100);

-- SQL Server到MySQL的重命名字段 (使用标准SQL语法)
ALTER TABLE ddlTestEmployee CHANGE COLUMN last_name surname NVARCHAR(50);

-- MySQL到SQL Server的DDL测试语句

-- MySQL到SQL Server的ADD COLUMN
ALTER TABLE ddlTestEmployee ADD COLUMN bonus DECIMAL(10,2);

-- MySQL到SQL Server的MODIFY COLUMN
ALTER TABLE ddlTestEmployee MODIFY COLUMN first_name VARCHAR(100);

-- MySQL到SQL Server的重命名字段
ALTER TABLE ddlTestEmployee CHANGE COLUMN last_name surname VARCHAR(50);

-- 数据类型映射测试
-- SQL Server NVARCHAR(50) -> MySQL VARCHAR(50)
ALTER TABLE ddlTestEmployee ADD COLUMN nickname NVARCHAR(50);

-- SQL Server DATETIME2 -> MySQL DATETIME
ALTER TABLE ddlTestEmployee ADD COLUMN updated_at DATETIME2;

-- SQL Server DECIMAL(10,2) -> MySQL DECIMAL(10,2)
ALTER TABLE ddlTestEmployee ADD COLUMN commission DECIMAL(10,2);

-- SQL Server INT -> MySQL INT
ALTER TABLE ddlTestEmployee ADD COLUMN level INT;

-- SQL Server BIT -> MySQL TINYINT(1)
ALTER TABLE ddlTestEmployee ADD COLUMN is_active BIT;

-- MySQL VARCHAR(50) -> SQL Server NVARCHAR(50)
ALTER TABLE ddlTestEmployee ADD COLUMN middle_name VARCHAR(50);

-- MySQL DATETIME -> SQL Server DATETIME2
ALTER TABLE ddlTestEmployee ADD COLUMN modified_at DATETIME;

-- MySQL DECIMAL(10,2) -> SQL Server DECIMAL(10,2)
ALTER TABLE ddlTestEmployee ADD COLUMN allowance DECIMAL(10,2);

-- MySQL INT -> SQL Server INT
ALTER TABLE ddlTestEmployee ADD COLUMN grade INT;

-- MySQL TINYINT(1) -> SQL Server BIT
ALTER TABLE ddlTestEmployee ADD COLUMN is_manager TINYINT(1);

-- 语法差异处理测试
-- SQL Server语法
ALTER TABLE ddlTestEmployee ADD sqlserver_col INT;

-- MySQL语法
ALTER TABLE ddlTestEmployee ADD COLUMN mysql_col INT;

-- 约束处理测试
-- SQL Server约束
ALTER TABLE ddlTestEmployee ADD CONSTRAINT uk_employee_name UNIQUE (first_name, last_name);

-- MySQL约束
ALTER TABLE ddlTestEmployee ADD CONSTRAINT uk_employee_name UNIQUE (first_name, last_name);