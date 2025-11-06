package org.dbsyncer.sdk.enums;

/**
 * 标准数据类型
 */
public enum DataTypeEnum {
    /** 文本 */
    STRING,
    UNICODE_STRING,
    /** 整型 */
    BYTE,
    UNSIGNED_BYTE,
    SHORT,
    UNSIGNED_SHORT,
    INT,
    UNSIGNED_INT,
    LONG,
    UNSIGNED_LONG,
    /** 浮点型 */
    DECIMAL,
    UNSIGNED_DECIMAL,
    DOUBLE,
    FLOAT,
    /** 布尔型 */
    BOOLEAN,
    /** 时间 */
    DATE,
    TIME,
    TIMESTAMP,
    /** 二进制 */
    BYTES,
    /** 结构化文本 */
    JSON,
    XML,
    /** 大文本 */
    TEXT,
    UNICODE_TEXT,
    /** 枚举和集合 */
    ENUM,
    SET
}