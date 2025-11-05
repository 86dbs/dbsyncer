package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnsignedIntType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL 无符号整型支持
 */
public final class MySQLUnsignedIntType extends UnsignedIntType {

    private enum TypeEnum {
        MEDIUMINT_UNSIGNED("MEDIUMINT UNSIGNED"),
        INT_UNSIGNED("INT UNSIGNED"),
        INTEGER_UNSIGNED("INTEGER UNSIGNED");

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
    protected Long merge(Object val, Field field) {
        if (val instanceof Number) {
            Number num = (Number) val;
            long longVal = num.longValue();
            // 处理可能的负数（JDBC可能返回负数）
            if (longVal < 0) {
                longVal = longVal & 0xFFFFFFFFL; // 转换为无符号
            }
            return Math.min(longVal, 4294967295L);
        }
        return throwUnsupportedException(val, field);
    }
}

