-- 重置测试表结构脚本 (SQL Server)
-- 用于在测试之间恢复表结构到初始状态，确保测试间的隔离性
-- 适用于：Microsoft SQL Server

-- 删除并重建ddlTestEmployee表（用于DDL同步集成测试）
IF OBJECT_ID('ddlTestEmployee', 'U') IS NOT NULL 
    DROP TABLE ddlTestEmployee;

CREATE TABLE ddlTestEmployee (
    id INT IDENTITY(1,1) PRIMARY KEY,
    first_name NVARCHAR(50) NOT NULL,
    last_name NVARCHAR(50),
    department NVARCHAR(100),
    created_at DATETIME2 DEFAULT GETDATE()
);

-- 清除可能存在的旧数据
DELETE FROM ddlTestEmployee;

