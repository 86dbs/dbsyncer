/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.clickhouse.schema.support;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimestampType;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-05-29 23:50
 */
public final class ClickHouseTimestampType extends TimestampType {

    private enum TypeEnum {
        DATETIME("datetime"),
        DATETIME64("datetime64");

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
        if (val instanceof Timestamp) {
            return (Timestamp) val;
        }
        if (val instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) val);
        }
        if (val instanceof OffsetDateTime) {
            return Timestamp.from(((OffsetDateTime) val).toInstant());
        }
        if (val instanceof Date) {
            return new Timestamp(((Date) val).getTime());
        }
        if (val instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) val).getTime());
        }
        if (val instanceof Number) {
            return new Timestamp(((Number) val).longValue());
        }
        if (val instanceof String) {
            String s = ((String) val).trim();
            if (isZeroDateTime(s)) {
                return null;
            }
            Timestamp timestamp = DateFormatUtil.stringToTimestamp(s);
            if (timestamp != null) {
                return timestamp;
            }
        }
        if (val instanceof byte[]) {
            Timestamp timestamp = DateFormatUtil.stringToTimestamp(new String((byte[]) val));
            if (timestamp != null) {
                return timestamp;
            }
        }
        return throwUnsupportedException(val, field);
    }

    private boolean isZeroDateTime(String value) {
        return StringUtil.equals(value, "0000-00-00 00:00:00")
                || StringUtil.equals(value, "0000-00-00");
    }
}
