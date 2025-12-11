-- 清理测试数据脚本 (SQL Server)
-- 用于在测试类结束后删除测试表和数据
-- 适用于：Microsoft SQL Server

-- 删除测试表（源表和目标表）
IF OBJECT_ID('ddlTestSource', 'U') IS NOT NULL 
    DROP TABLE ddlTestSource;

IF OBJECT_ID('ddlTestTarget', 'U') IS NOT NULL 
    DROP TABLE ddlTestTarget;

