/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DoubleType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-25 23:26
 */
public class PostgreSQLDoubleType extends DoubleType {
    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("FLOAT8"));
    }

    @Override
    protected Double merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }
}