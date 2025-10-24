/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BooleanType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-25 23:26
 */
public class PostgreSQLBooleanType extends BooleanType {
    private enum TypeEnum {
        BOOL("bool");

        private final String value;

        TypeEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getValue).collect(Collectors.toSet());
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