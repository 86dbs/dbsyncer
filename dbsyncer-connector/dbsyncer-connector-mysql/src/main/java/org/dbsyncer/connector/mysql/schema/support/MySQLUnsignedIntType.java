package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnsignedIntType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * MySQL 无符号整型支持
 */
public final class MySQLUnsignedIntType extends UnsignedIntType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("MEDIUMINT UNSIGNED", "INT UNSIGNED", "INTEGER UNSIGNED"));
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

