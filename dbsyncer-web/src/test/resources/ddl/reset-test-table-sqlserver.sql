-- 重置测试表结构脚本 (SQL Server)
-- 用于在测试之间恢复表结构到初始状态，确保测试间的隔离性
-- 适用于：Microsoft SQL Server
-- 注意：此脚本会同时应用到源数据库和目标数据库
-- 源数据库创建 ddlTestSource 表，目标数据库创建 ddlTestTarget 表

-- 删除并重建源表 ddlTestSource（用于DDL同步集成测试）
-- DROP TABLE 会自动重置 IDENTITY 种子，确保每次测试都从 id=1 开始
IF OBJECT_ID('ddlTestSource', 'U') IS NOT NULL 
    DROP TABLE ddlTestSource;

CREATE TABLE ddlTestSource (
    id INT IDENTITY(1,1) PRIMARY KEY,
    first_name NVARCHAR(50) NOT NULL,
    last_name NVARCHAR(50),
    department NVARCHAR(100),
    created_at DATETIME2 DEFAULT GETDATE()
);

-- 删除并重建目标表 ddlTestTarget（用于DDL同步集成测试）
IF OBJECT_ID('ddlTestTarget', 'U') IS NOT NULL 
    DROP TABLE ddlTestTarget;

CREATE TABLE ddlTestTarget (
    id INT IDENTITY(1,1) PRIMARY KEY,
    first_name NVARCHAR(50) NOT NULL,
    last_name NVARCHAR(50),
    department NVARCHAR(100),
    created_at DATETIME2 DEFAULT GETDATE()
);

