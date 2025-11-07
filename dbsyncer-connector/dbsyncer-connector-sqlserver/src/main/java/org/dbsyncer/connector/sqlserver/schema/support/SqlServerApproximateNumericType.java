/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DoubleType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SQL Server 近似数值类型
 * 包括浮点数类型
 * 注意：近似数值类型不支持 IDENTITY 属性
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class SqlServerApproximateNumericType extends DoubleType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("FLOAT", "REAL"));
    }

    @Override
    protected Double merge(Object val, Field field) {
        if (val instanceof Double) {
            return (Double) val;
        }
        if (val instanceof Float) {
            return ((Float) val).doubleValue();
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

