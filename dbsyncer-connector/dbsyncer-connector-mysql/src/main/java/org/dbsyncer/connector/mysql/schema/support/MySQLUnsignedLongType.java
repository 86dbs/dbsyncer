package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnsignedLongType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL 无符号长整型支持
 */
public final class MySQLUnsignedLongType extends UnsignedLongType {

    private enum TypeEnum {
        BIGINT_UNSIGNED("BIGINT UNSIGNED");

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
    protected BigDecimal merge(Object val, Field field) {
        if (val instanceof Number) {
            return new BigDecimal(val.toString());
        }
        return throwUnsupportedException(val, field);
    }
}

