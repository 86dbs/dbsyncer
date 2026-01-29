/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import microsoft.sql.DateTimeOffset;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimestampType;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-11 22:21
 */
public final class SqlServerTimestampType extends TimestampType {

    private enum TypeEnum {
        DATETIME("datetime"),
        DATETIME2("datetime2");

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
        if (val instanceof Long) {
            return new Timestamp((Long) val);
        }

        if (val instanceof DateTimeOffset) {
            LocalDateTime dateTime = ((DateTimeOffset) val).getOffsetDateTime().toLocalDateTime();
            return Timestamp.valueOf(dateTime);
        }

        if (val instanceof String) {
            Timestamp timestamp = DateFormatUtil.stringToTimestamp((String) val);
            if (null != timestamp) {
                return timestamp;
            }
        }

        return throwUnsupportedException(val, field);
    }

}
