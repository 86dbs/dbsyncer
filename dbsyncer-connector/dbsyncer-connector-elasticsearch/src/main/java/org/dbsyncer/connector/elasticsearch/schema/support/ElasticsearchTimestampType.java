/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.schema.support;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimestampType;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.DateTimeException;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ES 日期类型
 * 支持: date
 * 默认格式："strict_date_optional_time||epoch_millis"
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-11 22:21
 */
public final class ElasticsearchTimestampType extends TimestampType {

    private enum TypeEnum {

        DATE("date");

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
        if (val instanceof String) {
            String format = (String) field.getExtInfo().get("format");
            if (format != null) {
                try {
                    if (format.contains(DateFormatUtil.PATTERN_YYYY_MM_DD_HH_MM_SS)) {
                        return DateFormatUtil.stringToTimestamp((String) val, DateTimeFormatter.ofPattern(format));
                    }
                    if (format.equals(DateFormatUtil.PATTERN_YYYY_MM_DD)) {
                        return new Timestamp(DateFormatUtil.stringToDate((String) val).getTime());
                    }
                } catch (ParseException | DateTimeException e) {
                    // 尝试走兜底解析
                }
            }
            return DateFormatUtil.stringToTimestamp((String) val);
        }

        if (val instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) val).getTime());
        }

        if (val instanceof Long) {
            return new Timestamp((Long) val);
        }

        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        // 根据配置转换时间格式
        String format = (String) field.getExtInfo().get("format");
        // ES date 类型只能识别 java.util.Date
        if (val instanceof Timestamp) {
            return formatDate((Timestamp) val, format);
        }

        if (val instanceof Date) {
            Date date = (Date) val;
            return formatDate(new Timestamp(date.getTime()), format);
        }

        // 如果已经是 java.util.Date，直接返回
        if (val instanceof java.util.Date) {
            java.util.Date date = (java.util.Date) val;
            return formatDate(new Timestamp(date.getTime()), format);
        }

        if (val instanceof Long) {
            return formatDate(new Timestamp((Long) val), format);
        }

        if (val instanceof String) {
            return formatDate(DateFormatUtil.stringToTimestamp((String) val), format);
        }
        return throwUnsupportedException(val, field);
    }

    private Object formatDate(Timestamp timestamp, String format) {
        if (StringUtil.isNotBlank(format)) {
            return DateFormatUtil.timestampToString(timestamp, DateTimeFormatter.ofPattern(format));
        }
        return new java.util.Date(timestamp.getTime());
    }
}
