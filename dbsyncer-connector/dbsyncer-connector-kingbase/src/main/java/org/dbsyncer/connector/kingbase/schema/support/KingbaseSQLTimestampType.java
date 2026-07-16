/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.kingbase.schema.support;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimestampType;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-25 23:26
 */
public final class KingbaseSQLTimestampType extends TimestampType {

    private enum TypeEnum {

        TIMESTAMP("timestamp"), TIMESTAMP_TZ("timestamptz");

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
        if (val instanceof OffsetDateTime) {
            OffsetDateTime offsetDateTime = (OffsetDateTime) val;
            return Timestamp.from(offsetDateTime.toInstant());
        }
        if (val instanceof Timestamp) {
           return DateFormatUtil.stringToTimestamp((String) val);
        }
        return throwUnsupportedException(val, field);
    }
}