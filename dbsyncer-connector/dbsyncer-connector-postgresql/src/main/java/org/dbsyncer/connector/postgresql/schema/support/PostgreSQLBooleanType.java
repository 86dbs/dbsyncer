/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BooleanType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-25 23:26
 */
public class PostgreSQLBooleanType extends BooleanType {
    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("BOOL"));
    }

    @Override
    protected Boolean merge(Object val, Field field) {
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