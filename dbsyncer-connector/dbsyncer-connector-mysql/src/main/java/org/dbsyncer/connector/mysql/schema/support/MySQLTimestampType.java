/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

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
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:59
 */
public final class MySQLTimestampType extends TimestampType {

    private enum TypeEnum {
        DATETIME, TIMESTAMP;
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected Timestamp merge(Object val, Field field) {
        if (val instanceof java.util.Date) {
            java.util.Date date = (java.util.Date) val;
            return new Timestamp(date.getTime());
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (isDatetime(field)) {
            int fsp = resolveFractionalSeconds(field);
            if (val instanceof Timestamp) {
                return DateFormatUtil.timestampToString((Timestamp) val, DateFormatUtil.getDatetimeFormatter(fsp));
            }
            if (val instanceof LocalDateTime) {
                return ((LocalDateTime) val).format(DateFormatUtil.getDatetimeFormatter(fsp));
            }
        }
        if (val instanceof String) {
            if (StringUtil.equals((String) val, "0000-00-00 00:00:00")) {
                return val;
            }
        }
        return super.convert(val, field);
    }

    private boolean isDatetime(Field field) {
        return StringUtil.equalsIgnoreCase(TypeEnum.DATETIME.name(), field.getTypeName());
    }

    /**
     * 解析 DATETIME 小数秒精度
     */
    private int resolveFractionalSeconds(Field field) {
        int ratio = field.getRatio();
        if (ratio > 0) {
            return Math.min(6, ratio);
        }
        int columnSize = field.getColumnSize();
        if (columnSize > 19) {
            return Math.min(6, columnSize - 20);
        }
        return 0;
    }
}
