CREATE TABLE DataType (
    ID int IDENTITY(1,1) PRIMARY KEY,
    VarCharColumn varchar(50),
    CharColumn char(3),
    NTextColumn nvarchar(max),
    IntColumn int,
    DecimalColumn decimal(10, 2),
    FloatColumn float,
    MoneyColumn money,
    DateColumn date,
    DateTimeColumn datetime,
    BitColumn bit,
    BinaryColumn varbinary(max),
    XmlColumn xml,
    JsonColumn nvarchar(max)
);

INSERT INTO DataType (
    VarCharColumn, CharColumn, NTextColumn, IntColumn, DecimalColumn, FloatColumn, MoneyColumn,
    DateColumn, DateTimeColumn, BitColumn, BinaryColumn, XmlColumn, JsonColumn
) VALUES
(
    '常规记录', 'CH1', N'这是一段Unicode长文本，用于测试nvarchar(max)类型。',
    42, 1234.56, 789.012, 99.99,
    '2025-10-23', '2025-10-23 16:11:33', 1,
    0x4D5A, -- 一个简短的二进制示例（如文件头）
    '<note><to>测试员</to><body>这是一段XML内容</body></note>',
    '{"name": "Test", "active": true}'
),
(
    '', 'MAX', N'',
    -2147483648, 99999999.99, -1.79E+308, 999999.99,
    '1991-01-01', '1753-01-01 00:00:00', 0,
    NULL,
    '<root />',
    '{"array": [1, 2, 3], "nullField": null}'
),
(
    '特殊&字符%', 'S2', N'Unicode文字：🍀',
    0, 0.00, 0.0, 0.00,
    GETDATE(), GETDATE(), 0,
    0x00,
    '<?xml version="1.0"?><test />',
    '{"id": 0, "values": []}'
);