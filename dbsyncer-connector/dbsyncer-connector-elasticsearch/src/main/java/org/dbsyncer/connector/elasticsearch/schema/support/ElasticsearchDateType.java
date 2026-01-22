/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.schema.support;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DateType;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ES 日期类型
 * 支持: date, date_range
 * 默认格式："strict_date_optional_time||epoch_millis"
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-11 22:21
 */
public final class ElasticsearchDateType extends DateType {

    private enum TypeEnum {
        DATE("date"),
        DATE_RANGE("date_range");

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
        if (val instanceof String) {
            return DateFormatUtil.stringToDate((String) val);
        }

        if (val instanceof Timestamp) {
            return new Date(((Timestamp) val).getTime());
        }

        if (val instanceof java.util.Date) {
            return new Date(((java.util.Date) val).getTime());
        }

        if (val instanceof Long) {
            return new Date((Long) val);
        }

        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        // ES date 类型只能识别 java.util.Date
        if (val instanceof Timestamp) {
            return new java.util.Date(((Timestamp) val).getTime());
        }

        if (val instanceof Date) {
            return new java.util.Date(((Date) val).getTime());
        }

        // 如果已经是 java.util.Date，直接返回
        if (val instanceof java.util.Date) {
            return val;
        }

        if (val instanceof Long) {
            return new java.util.Date((Long) val);
        }

        if (val instanceof Integer) {
            return new java.util.Date(Long.parseLong(val.toString()));
        }

        if (val instanceof String) {
            Timestamp timestamp = DateFormatUtil.stringToTimestamp((String) val);
            if (null != timestamp) {
                return new java.util.Date(timestamp.getTime());
            }
        }
        return throwUnsupportedException(val, field);
    }
}
