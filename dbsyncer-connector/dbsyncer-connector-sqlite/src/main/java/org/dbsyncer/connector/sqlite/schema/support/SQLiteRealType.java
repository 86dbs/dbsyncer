/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlite.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DoubleType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SQLite REAL 存储类 - 实数亲和性
 * 支持所有浮点数相关的类型声明
 * <p>
 * <b>类型分析：</b>
 * <ul>
 *   <li><b>REAL</b> - 原生存储类。浮点数，存储为 8 字节的 IEEE 浮点数</li>
 *   <li><b>DOUBLE</b> - 非原生类型，映射到 REAL 亲和性。双精度浮点数</li>
 *   <li><b>DOUBLE PRECISION</b> - 非原生类型，映射到 REAL 亲和性。双精度浮点数（功能与 DOUBLE 相同）</li>
 *   <li><b>FLOAT</b> - 非原生类型，映射到 REAL 亲和性。浮点类型</li>
 *   <li><b>NUMERIC</b> - 非原生类型，映射到 NUMERIC 亲和性（实际存储为 REAL）。数值类型，支持精确小数</li>
 *   <li><b>DECIMAL</b> - 非原生类型，映射到 NUMERIC 亲和性（实际存储为 REAL）。小数类型，支持精确小数</li>
 * </ul>
 * REAL 和 NUMERIC 亲和性类型最终都存储为 REAL 存储类（8 字节 IEEE 浮点数）。
 * </p>
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class SQLiteRealType extends DoubleType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("REAL", "DOUBLE", "FLOAT", "NUMERIC", "DECIMAL", "DOUBLE PRECISION"));
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
