/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.dameng.schema.support;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimestampType;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 达梦日期时间类型（归一化后不含精度括号）
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-07-18
 */
public final class DamengTimestampType extends TimestampType {

    private enum TypeEnum {
        TIMESTAMP("TIMESTAMP"),
        DATETIME("DATETIME"),
        DATETIME2("DATETIME2"),
        TIMESTAMP_WITH_TIME_ZONE("TIMESTAMP WITH TIME ZONE"),
        TIMESTAMP_WITH_LOCAL_TIME_ZONE("TIMESTAMP WITH LOCAL TIME ZONE"),
        DATETIME_WITH_TIME_ZONE("DATETIME WITH TIME ZONE"),
        DATETIME2_WITH_TIME_ZONE("DATETIME2 WITH TIME ZONE");

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
        if (val instanceof Date) {
            return new Timestamp(((Date) val).getTime());
        }
        if (val instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) val).getTime());
        }
        if (val instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) val);
        }
        if (val instanceof String) {
            Timestamp timestamp = DateFormatUtil.stringToTimestamp((String) val);
            if (timestamp != null) {
                return timestamp;
            }
        }
        if (val instanceof Number) {
            return new Timestamp(((Number) val).longValue());
        }
        return throwUnsupportedException(val, field);
    }
}
