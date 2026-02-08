/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.schema.support;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DateType;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-25 00:03
 */
public final class OracleDateType extends DateType {

    private enum TypeEnum {

        DATE("DATE");

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
    protected Date merge(Object val, Field field) {
        if (val instanceof Date) {
            return (Date) val;
        }
        if (val instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) val;
            return Date.valueOf(timestamp.toLocalDateTime().toLocalDate());
        }
        if (val instanceof java.util.Date) {
            java.util.Date date = (java.util.Date) val;
            return new Date(date.getTime());
        }
        if (val instanceof LocalDateTime) {
            LocalDateTime dateTime = (LocalDateTime) val;
            return Date.valueOf(dateTime.toLocalDate());
        }
        if (val instanceof String) {
            String s = (String) val;
            // 先尝试解析为 Timestamp（支持带时间的字符串）
            Timestamp timestamp = DateFormatUtil.stringToTimestamp(s);
            if (timestamp != null) {
                return Date.valueOf(timestamp.toLocalDateTime().toLocalDate());
            }
            // 再尝试直接解析为 Date（仅日期格式）
            return DateFormatUtil.stringToDate(s);
        }
        return throwUnsupportedException(val, field);
    }
}
