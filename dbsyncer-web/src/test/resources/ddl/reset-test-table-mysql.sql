-- 重置测试表结构脚本 (MySQL)
-- 用于在测试之间恢复表结构到初始状态，确保测试间的隔离性
-- 适用于：MySQL, MariaDB

-- 删除并重建源表（用于DDL同步集成测试）
DROP TABLE IF EXISTS ddlTestSource;
DROP TABLE IF EXISTS ddlTestTarget;

-- 创建源表
CREATE TABLE IF NOT EXISTS ddlTestSource (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50),
    department VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建目标表（初始结构与源表相同）
CREATE TABLE IF NOT EXISTS ddlTestTarget (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50),
    department VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 清除可能存在的旧数据
DELETE FROM ddlTestSource;
DELETE FROM ddlTestTarget;

