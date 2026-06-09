/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.h2.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimestampType;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * H2 时间戳类型
 */
public final class H2TimestampType extends TimestampType {

    private enum TypeEnum {

        TIMESTAMP("TIMESTAMP"),
        DATETIME("DATETIME"),
        TIMESTAMP_WITH_TIME_ZONE("TIMESTAMP WITH TIME ZONE");

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
