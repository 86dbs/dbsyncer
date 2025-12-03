-- 清理测试数据脚本 (SQL Server)
-- 用于在测试类结束后删除测试表和数据
-- 适用于：Microsoft SQL Server

-- 删除测试表
IF OBJECT_ID('ddlTestEmployee', 'U') IS NOT NULL 
    DROP TABLE ddlTestEmployee;

