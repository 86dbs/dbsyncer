-- SQL Server测试数据
-- 创建测试表
CREATE TABLE employee (
    id INT IDENTITY(1,1) PRIMARY KEY,
    first_name NVARCHAR(50) NOT NULL,
    last_name NVARCHAR(50),
    department NVARCHAR(100),
    created_at DATETIME2 DEFAULT GETDATE()
);

-- 插入测试数据
INSERT INTO employee (first_name, last_name, department) VALUES 
(N'张三', N'张', N'技术部'),
(N'李四', N'李', N'市场部'),
(N'王五', N'王', N'人事部');

-- 测试DDL语句
-- 新增字段
ALTER TABLE employee ADD salary DECIMAL(10,2);

-- 修改字段
ALTER TABLE employee ALTER COLUMN first_name NVARCHAR(100);

-- 重命名字段 (使用sp_rename存储过程)
EXEC sp_rename 'employee.last_name', 'surname', 'COLUMN';

-- 删除字段
ALTER TABLE employee DROP COLUMN created_at;