-- 清理测试数据脚本 (MySQL)
-- 用于在测试类结束后删除测试表和数据
-- 适用于：MySQL, MariaDB

-- 删除测试表（源表和目标表）
DROP TABLE IF EXISTS ddlTestSource;
DROP TABLE IF EXISTS ddlTestTarget;

