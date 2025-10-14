/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlite.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DoubleType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQLite REAL 存储类 - 实数亲和性
 * 支持所有浮点数相关的类型声明
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class SQLiteRealType extends DoubleType {

    private enum TypeEnum {
        // REAL 亲和性类型
        REAL,        // 实数类型
        DOUBLE,      // 双精度浮点
        FLOAT,       // 浮点类型
        // NUMERIC 亲和性类型（映射到 REAL）
        NUMERIC,     // 数值类型
        DECIMAL      // 小数类型
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected Double merge(Object val, Field field) {
        if (val instanceof Double) {
            return (Double) val;
        }
        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        }
        if (val instanceof String) {
            try {
                return Double.parseDouble((String) val);
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
        return 0.0;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Double) {
            return val;
        }
        if (val instanceof Float) {
            return ((Float) val).doubleValue();
        }
        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        }
        return super.convert(val, field);
    }
}
