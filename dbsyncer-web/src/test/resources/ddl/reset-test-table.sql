-- 重置测试表结构脚本
-- 用于在测试之间恢复表结构到初始状态，确保测试间的隔离性

-- 删除并重建ddlTestTable表（用于DDL同步集成测试）
DROP TABLE IF EXISTS ddlTestTable;

CREATE TABLE IF NOT EXISTS ddlTestTable (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 清除可能存在的旧数据
DELETE FROM ddlTestTable;

