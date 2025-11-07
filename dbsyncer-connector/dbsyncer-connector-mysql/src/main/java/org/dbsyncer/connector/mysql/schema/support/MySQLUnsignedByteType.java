package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnsignedByteType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * MySQL 无符号字节类型支持
 */
public final class MySQLUnsignedByteType extends UnsignedByteType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("TINYINT UNSIGNED"));
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

