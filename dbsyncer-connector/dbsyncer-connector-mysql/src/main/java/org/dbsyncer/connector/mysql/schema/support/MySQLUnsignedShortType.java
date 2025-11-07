package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnsignedShortType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * MySQL 无符号短整型支持
 */
public final class MySQLUnsignedShortType extends UnsignedShortType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("SMALLINT UNSIGNED"));
    }

    @Override
    protected Integer merge(Object val, Field field) {
        if (val instanceof Number) {
            Number num = (Number) val;
            int intVal = num.intValue();
            // 处理可能的负数（JDBC可能返回负数）
            if (intVal < 0) {
                intVal = intVal & 0xFFFF; // 转换为无符号
            }
            return Math.min(intVal, 65535);
        }
        return throwUnsupportedException(val, field);
    }
}

