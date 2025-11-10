/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BytesType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-10-22 01:27
 */
public class PostgreSQLBytesType extends BytesType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("BYTEA"));
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }

}