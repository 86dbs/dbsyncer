package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnsignedByteType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL 无符号字节类型支持
 */
public final class MySQLUnsignedByteType extends UnsignedByteType {

    private enum TypeEnum {
        TINYINT_UNSIGNED("TINYINT UNSIGNED");

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
    protected Short merge(Object val, Field field) {
        if (val instanceof Number) {
            Number num = (Number) val;
            int intVal = num.intValue();
            // 处理可能的负数（JDBC可能返回负数）
            if (intVal < 0) {
                intVal = intVal & 0xFF; // 转换为无符号
            }
            return (short) Math.min(intVal, 255);
        }
        return throwUnsupportedException(val, field);
    }
}

