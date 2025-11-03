-- 初始化测试数据脚本
-- 用于在测试开始前创建测试表和初始数据

-- MySQL源表
CREATE TABLE IF NOT EXISTS ddlTestUserInfo (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- MySQL目标表
CREATE TABLE IF NOT EXISTS ddlTestUserInfo (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SQL Server源表
CREATE TABLE IF NOT EXISTS ddlTestEmployee (
    id INT IDENTITY(1,1) PRIMARY KEY,
    first_name NVARCHAR(50) NOT NULL,
    last_name NVARCHAR(50),
    department NVARCHAR(100),
    created_at DATETIME2 DEFAULT GETDATE()
);

-- MySQL目标表 (从SQL Server同步)
CREATE TABLE IF NOT EXISTS ddlTestEmployee (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50),
    department VARCHAR(100),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 通用测试表
CREATE TABLE IF NOT EXISTS ddlTestTable (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 清除可能存在的旧数据
DELETE FROM ddlTestUserInfo;
DELETE FROM ddlTestEmployee;
DELETE FROM ddlTestTable;

-- 插入初始数据
-- MySQL用户信息表初始数据
INSERT INTO ddlTestUserInfo (username, email) VALUES 
('test_user1', 'test1@example.com'),
('test_user2', 'test2@example.com'),
('test_user3', 'test3@example.com');

-- SQL Server员工表初始数据
INSERT INTO ddlTestEmployee (first_name, last_name, department) VALUES 
('John', 'Doe', 'Engineering'),
('Jane', 'Smith', 'Marketing'),
('Bob', 'Johnson', 'Sales');

-- 通用测试表初始数据
INSERT INTO ddlTestTable (name) VALUES 
('test_name1'),
('test_name2'),
('test_name3');