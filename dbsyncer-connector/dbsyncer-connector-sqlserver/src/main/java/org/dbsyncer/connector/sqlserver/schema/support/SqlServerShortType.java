/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.ShortType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SQL Server SMALLINT 类型
 * 映射到标准类型 SHORT
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2025-01-27
 */
public final class SqlServerShortType extends ShortType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("SMALLINT", "SMALLINT IDENTITY"));
    }

    @Override
    protected Short merge(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).shortValue();
        }
        return throwUnsupportedException(val, field);
    }

}

