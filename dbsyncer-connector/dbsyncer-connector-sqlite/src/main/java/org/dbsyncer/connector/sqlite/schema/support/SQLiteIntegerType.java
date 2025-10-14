/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlite.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.LongType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQLite INTEGER 存储类 - 整数亲和性
 * 支持所有整数相关的类型声明
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class SQLiteIntegerType extends LongType {

    private enum TypeEnum {
        // INTEGER 亲和性类型
        INTEGER,     // 整数类型
        INT,         // 整数别名
        TINYINT,     // 小整数
        SMALLINT,    // 小整数
        MEDIUMINT,   // 中等整数
        BIGINT,      // 大整数
        // 其他映射到 INTEGER 亲和性的类型
        BOOLEAN      // 布尔值（存储为 0 或 1）
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected Long merge(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).longValue();
        }
        if (val instanceof Boolean) {
            return ((Boolean) val) ? 1L : 0L;
        }
        if (val instanceof String) {
            try {
                return Long.parseLong((String) val);
            } catch (NumberFormatException e) {
                return 0L;
            }
        }
        return 0L;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Long) {
            return val;
        }
        if (val instanceof Integer) {
            return ((Integer) val).longValue();
        }
        if (val instanceof Boolean) {
            return ((Boolean) val) ? 1L : 0L;
        }
        return super.convert(val, field);
    }
}
