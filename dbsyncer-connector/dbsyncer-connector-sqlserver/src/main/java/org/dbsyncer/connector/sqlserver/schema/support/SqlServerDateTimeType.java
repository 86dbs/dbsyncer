/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimestampType;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQL Server 日期和时间类型
 * 包括所有日期时间相关的类型
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class SqlServerDateTimeType extends TimestampType {

    private enum TypeEnum {
        DATE,              // 日期类型 (YYYY-MM-DD)
        TIME,              // 时间类型 (HH:MM:SS.nnnnnnn)
        DATETIME,          // 日期时间类型 (YYYY-MM-DD HH:MM:SS.nnn)
        DATETIME2,         // 日期时间2类型 (YYYY-MM-DD HH:MM:SS.nnnnnnn)
        SMALLDATETIME,     // 小日期时间类型 (YYYY-MM-DD HH:MM:SS)
        DATETIMEOFFSET,    // 日期时间偏移类型 (带时区信息)
        TIMESTAMP          // 时间戳类型 (自动生成的唯一时间戳)
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected Timestamp merge(Object val, Field field) {
        if (val instanceof Timestamp) {
            return (Timestamp) val;
        }
        if (val instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) val);
        }
        if (val instanceof String) {
            try {
                return Timestamp.valueOf((String) val);
            } catch (IllegalArgumentException e) {
                return new Timestamp(System.currentTimeMillis());
            }
        }
        if (val instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) val).getTime());
        }
        return new Timestamp(System.currentTimeMillis());
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Timestamp) {
            return val;
        }
        if (val instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) val);
        }
        if (val instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) val).getTime());
        }
        return super.convert(val, field);
    }
}
