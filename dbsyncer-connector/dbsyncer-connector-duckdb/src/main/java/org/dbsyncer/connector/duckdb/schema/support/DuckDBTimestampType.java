/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.duckdb.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimestampType;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DuckDB 时间戳类型
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-16 10:00
 */
public final class DuckDBTimestampType extends TimestampType {

    private enum TypeEnum {
        TIMESTAMP("TIMESTAMP"),
        DATETIME("DATETIME"),
        TIMESTAMP_TZ("TIMESTAMP WITH TIME ZONE"),
        TIMESTAMPTZ("TIMESTAMPTZ");

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
    protected Timestamp merge(Object val, Field field) {
        if (val instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) val);
        }
        if (val instanceof OffsetDateTime) {
            return Timestamp.from(((OffsetDateTime) val).toInstant());
        }
        return throwUnsupportedException(val, field);
    }
}
