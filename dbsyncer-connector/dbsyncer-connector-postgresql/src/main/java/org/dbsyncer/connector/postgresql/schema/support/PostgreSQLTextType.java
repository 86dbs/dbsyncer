package org.dbsyncer.connector.postgresql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnicodeTextType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * PostgreSQL TEXT类型支持
 * <p>
 * PostgreSQL的TEXT类型默认支持UTF-8，
 * 因此标准化为UNICODE_TEXT以确保数据安全性和跨数据库兼容性。
 * </p>
 */
public final class PostgreSQLTextType extends UnicodeTextType {

    private enum TypeEnum {
        TEXT("text");

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
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            return (String) val;
        }
        return throwUnsupportedException(val, field);
    }
}