/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimestampType;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-25 23:26
 */
public class PostgreSQLTimestampType extends TimestampType {
    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("TIMESTAMP", "TIMESTAMPTZ"));
    }

    @Override
    protected Timestamp merge(Object val, Field field) {
        if (val instanceof OffsetDateTime) {
            OffsetDateTime offsetDateTime = (OffsetDateTime) val;
            return Timestamp.from(offsetDateTime.toInstant());
        }
        return throwUnsupportedException(val, field);
    }
}