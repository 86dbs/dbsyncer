/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DoubleType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-11 22:21
 */
public final class SqlServerDoubleType extends DoubleType {

    private enum TypeEnum {
        FLOAT("float");

        private final String value;

        TypeEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    @Override
    protected Double merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(SqlServerDoubleType.TypeEnum.values()).map(SqlServerDoubleType.TypeEnum::getValue).collect(Collectors.toSet());
    }
    @Override
    protected  Object convert(Object val, Field field){
        if (val instanceof Number) {
            return ((Number) val).toString();
        }
        return throwUnsupportedException(val, field);
    }
}
