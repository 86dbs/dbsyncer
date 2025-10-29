-- 清理测试数据脚本
-- 用于在测试结束后删除测试表和数据

-- MySQL测试表清理
DROP TABLE IF EXISTS ddlTestUserInfo;
DROP TABLE IF EXISTS ddlTestEmployee;
DROP TABLE IF EXISTS ddlTestTable;

-- SQL Server测试表清理（如果需要单独处理SQL Server）
-- 注意：在实际使用中，可能需要根据数据库类型使用不同的语法
-- SQL Server的清理语句示例：
-- IF OBJECT_ID('ddlTestEmployee', 'U') IS NOT NULL DROP TABLE ddlTestEmployee;
-- IF OBJECT_ID('ddlTestUserInfo', 'U') IS NOT NULL DROP TABLE ddlTestUserInfo;
-- IF OBJECT_ID('ddlTestTable', 'U') IS NOT NULL DROP TABLE ddlTestTable;