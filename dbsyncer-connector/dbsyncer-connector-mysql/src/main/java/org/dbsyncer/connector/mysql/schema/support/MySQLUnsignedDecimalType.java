package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnsignedDecimalType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL 无符号精确小数类型支持
 */
public final class MySQLUnsignedDecimalType extends UnsignedDecimalType {

    private enum TypeEnum {
        DECIMAL_UNSIGNED("DECIMAL UNSIGNED");

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
            BigDecimal bd = new BigDecimal(val.toString());
            // 确保值 >= 0
            if (bd.compareTo(BigDecimal.ZERO) < 0) {
                return BigDecimal.ZERO;
            }
            return bd;
        }
        return throwUnsupportedException(val, field);
    }

}

