package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.IntType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SQL Server INT 类型
 * 映射到标准类型 INT
 */
public final class SqlServerIntType extends IntType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("INT", "INT IDENTITY", "INTEGER"));
    }

    @Override
    protected Integer merge(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).intValue();
        }
        return throwUnsupportedException(val, field);
    }

}

