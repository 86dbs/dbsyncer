package org.dbsyncer.connector.postgresql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.EnumType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * PostgreSQL ENUM类型支持
 */
public final class PostgreSQLEnumType extends EnumType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("ENUM"));
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            return (String) val;
        }
        return throwUnsupportedException(val, field);
    }
}