/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlite.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQLite TEXT 存储类 - 文本亲和性
 * 支持所有文本相关的类型声明
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class SQLiteTextType extends StringType {

    private enum TypeEnum {
        // TEXT 亲和性类型
        TEXT,        // 文本类型
        VARCHAR,     // 可变长度字符串
        CHAR,        // 固定长度字符串
        CLOB,        // 字符大对象
        // 其他映射到 TEXT 亲和性的类型
        DATETIME,    // 日期时间（存储为文本）
        DATE,        // 日期（存储为文本）
        TIME,        // 时间（存储为文本）
        TIMESTAMP    // 时间戳（存储为文本）
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            return (String) val;
        }
        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }
        return String.valueOf(val);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return val;
        }
        return super.convert(val, field);
    }
}
