package org.dbsyncer.connector.postgresql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.EnumType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * PostgreSQL ENUM类型支持
 */
public final class PostgreSQLEnumType extends EnumType {

    // PostgreSQL ENUM类型在JDBC中被识别为"user-defined"
    private enum TypeEnum {
        USER_DEFINED("user-defined");

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
        // PostgreSQL ENUM类型在JDBC中被识别为"user-defined"
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getValue).collect(Collectors.toSet());
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            return (String) val;
        }
        return throwUnsupportedException(val, field);
    }
}