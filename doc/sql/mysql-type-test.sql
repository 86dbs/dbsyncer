CREATE TABLE `data_type_test` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `col_tinyint` tinyint(4) DEFAULT NULL COMMENT '非常小的整数',
  `col_smallint` smallint(6) DEFAULT NULL COMMENT '小整数',
  `col_mediumint` mediumint(9) DEFAULT NULL COMMENT '中等整数',
  `col_int` int(11) DEFAULT NULL COMMENT '标准整数',
  `col_bigint` bigint(20) DEFAULT NULL COMMENT '大整数',
  `col_int_unsigned` int(10) unsigned DEFAULT NULL COMMENT '无符号整数',
  `col_float` float(10,4) DEFAULT NULL COMMENT '单精度浮点数',
  `col_double` double(10,4) DEFAULT NULL COMMENT '双精度浮点数',
  `col_decimal` decimal(15,6) DEFAULT NULL COMMENT '精确小数，适用于金融',
  `col_date` date DEFAULT NULL COMMENT '日期',
  `col_time` time DEFAULT NULL COMMENT '时间',
  `col_datetime` datetime DEFAULT NULL COMMENT '日期时间',
  `col_timestamp` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '时间戳，可自动更新',
  `col_year` year(4) DEFAULT NULL COMMENT '年份',
  `col_char` char(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '定长字符串',
  `col_varchar` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '变长字符串',
  `col_text` text COLLATE utf8mb4_unicode_ci COMMENT '长文本数据',
  `col_blob` blob COMMENT '二进制大对象',
  `col_enum` enum('值1','值2','值3') COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '枚举值，单选',
  `col_set` set('选项A','选项B','选项C') COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '集合，多选',
  `col_json` json DEFAULT NULL COMMENT 'JSON格式数据',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='MySQL数据类型测试表';


-- 插入包含各种数据类型的测试数据
INSERT INTO `data_type_test` (
  `col_tinyint`, `col_smallint`, `col_mediumint`, `col_int`, `col_bigint`,
  `col_int_unsigned`, `col_float`, `col_double`, `col_decimal`,
  `col_date`, `col_time`, `col_datetime`, `col_timestamp`, `col_year`,
  `col_char`, `col_varchar`, `col_text`,
  `col_blob`,
  `col_enum`, `col_set`,
  `col_json`
) VALUES
(
  -- 数值类型
  127, 32767, 8388607, 2147483647, 9223372036854775807, -- 有符号整数边界值
  4294967295, -- 无符号整数最大值
  123.4567, 123456.7890, 987654321.123456, -- 浮点数与定点数

  -- 日期时间类型
  '2025-10-21', '23:59:59', '2025-10-21 23:59:59', NOW(), 2025, -- 当前及特定时间

  -- 字符串类型
  '定长', '这是一个可变长度的字符串，用于测试VARCHAR类型。', REPEAT('这是一个很长的文本内容，用于测试TEXT类型的存储能力。', 10), -- 文本数据

  -- 二进制数据 (此处使用16进制表示法插入文本"Hello World"的二进制形式)
  0x48656C6C6F20576F726C64,

  -- 枚举与集合
  '值2', '选项A,选项C', -- ENUM单选，SET多选

  -- JSON数据
  '{"name": "测试用户", "age": 30, "isActive": true, "hobbies": ["阅读", "运动"], "address": {"city": "北京", "country": "中国"}}'
),
(
  -- 第二组数据：包含边界值、零值和NULL
  -128, -32768, -8388608, -2147483648, -9223372036854775808, -- 有符号整数最小值
  0, -- 无符号整数零值
  -123.4567, -123456.7890, -987654321.123456,

  '1971-01-01', '00:00:00', '1971-01-01 00:00:01', '1971-01-01 00:00:01', 1901, -- 最小日期时间

  'A', '', '', -- 空字符串测试

  NULL, -- BLOB字段为NULL

  '值1', '', -- SET为空

  NULL -- JSON字段为NULL
),
(
  -- 第三组数据：包含NULL值，用于测试空值处理
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL,
  NULL,
  NULL, NULL,
  NULL
);