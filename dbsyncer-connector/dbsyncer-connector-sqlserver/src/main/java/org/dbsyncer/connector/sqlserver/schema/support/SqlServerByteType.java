package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.ByteType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SQL Server TINYINT 类型
 * 映射到标准类型 BYTE
 */
public final class SqlServerByteType extends ByteType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("TINYINT", "TINYINT IDENTITY"));
    }

    @Override
    protected Byte merge(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).byteValue();
        }
        return throwUnsupportedException(val, field);
    }

}

