/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DoubleType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

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

    private enum TypeEnum {
        FLOAT,         // 浮点数 (8字节双精度)
        REAL           // 实数 (4字节单精度)
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

