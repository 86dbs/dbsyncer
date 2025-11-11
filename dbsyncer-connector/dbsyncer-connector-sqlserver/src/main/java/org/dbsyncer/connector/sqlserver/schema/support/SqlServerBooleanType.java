package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BooleanType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SQL Server BOOLEAN类型：用于布尔值
 * 支持 BIT 类型
 */
public final class SqlServerBooleanType extends BooleanType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("BIT"));
    }

    @Override
    protected Boolean merge(Object val, Field field) {
        if (val instanceof Boolean) {
            return (Boolean) val;
        }
        if (val instanceof Number) {
            Number num = (Number) val;
            return num.intValue() == 1 ? Boolean.TRUE : Boolean.FALSE;
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Boolean) {
            return val;
        }
        if (val instanceof Number) {
            Number num = (Number) val;
            return num.intValue() == 1 ? Boolean.TRUE : Boolean.FALSE;
        }
        return throwUnsupportedException(val, field);
    }
}

