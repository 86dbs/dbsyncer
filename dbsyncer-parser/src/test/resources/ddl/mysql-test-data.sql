-- MySQL测试数据
-- 创建测试表
CREATE TABLE user_info (
    id INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入测试数据
INSERT INTO user_info (username, email) VALUES 
('张三', 'zhangsan@example.com'),
('李四', 'lisi@example.com'),
('王五', 'wangwu@example.com');

-- 测试DDL语句
-- 新增字段
ALTER TABLE user_info ADD COLUMN phone VARCHAR(20) AFTER email;

-- 修改字段
ALTER TABLE user_info MODIFY COLUMN username VARCHAR(100) NOT NULL;

-- 重命名字段
ALTER TABLE user_info CHANGE COLUMN email user_email VARCHAR(100);

-- 删除字段
ALTER TABLE user_info DROP COLUMN created_at;