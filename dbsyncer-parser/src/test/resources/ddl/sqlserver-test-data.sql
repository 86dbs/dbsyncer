-- SQL Server测试数据

-- 源表
CREATE TABLE ddlTestEmployee (
    id INT IDENTITY(1,1) PRIMARY KEY,
    first_name NVARCHAR(50) NOT NULL,
    last_name NVARCHAR(50),
    department NVARCHAR(100),
    created_at DATETIME2 DEFAULT GETDATE()
);

-- 目标表 (从源表同步)
CREATE TABLE ddlTestEmployee (
    id INT IDENTITY(1,1) PRIMARY KEY,
    first_name NVARCHAR(50) NOT NULL,
    last_name NVARCHAR(50),
    department NVARCHAR(100),
    created_at DATETIME2 DEFAULT GETDATE()
);

-- SQL Server到SQL Server的DDL测试语句

-- ADD COLUMN
ALTER TABLE ddlTestEmployee ADD salary DECIMAL(10,2);

-- DROP COLUMN
ALTER TABLE ddlTestEmployee DROP COLUMN department;

-- ALTER COLUMN
ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(100);

-- CHANGE COLUMN (重命名字段，使用标准SQL语法)
ALTER TABLE ddlTestEmployee CHANGE COLUMN last_name surname NVARCHAR(50);