/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.doris.schema.support;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimestampType;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Doris 日期时间类型。
 * <p>
 * DATETIME / DATETIMEV2 / TIMESTAMP 写入前统一转为无时区墙钟字符串，
 * 避免 JDBC {@link Timestamp} 绑参或 Stream Load ISO 带时区串导致 ±8 小时偏移。
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-08-06
 */
public final class DorisTimestampType extends TimestampType {

    private enum TypeEnum {
        DATETIME, DATETIMEV2, TIMESTAMP;

        public String getValue() {
            return name();
        }
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getValue).collect(Collectors.toSet());
    }

    @Override
    protected Timestamp merge(Object val, Field field) {
        if (val instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) val).getTime());
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Timestamp) {
            return DateFormatUtil.timestampToString((Timestamp) val);
        }
        if (val instanceof LocalDateTime) {
            return ((LocalDateTime) val).format(DateFormatUtil.YYYY_MM_DD_HH_MM_SS);
        }
        if (val instanceof String) {
            if (StringUtil.equals((String) val, "0000-00-00 00:00:00")) {
                return val;
            }
        }
        Object converted = super.convert(val, field);
        if (converted instanceof Timestamp) {
            return DateFormatUtil.timestampToString((Timestamp) converted);
        }
        return converted;
    }
}
