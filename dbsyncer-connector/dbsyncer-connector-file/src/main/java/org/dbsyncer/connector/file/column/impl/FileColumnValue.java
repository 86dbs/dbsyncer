/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.file.column.impl;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.file.column.ColumnValue;

import java.sql.Date;
import java.sql.Timestamp;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-05 23:19
 */
public class FileColumnValue implements ColumnValue {

    private String value;

    @Override
    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public String asString() {
        return value;
    }

    @Override
    public Boolean asBoolean() {
        return "true".equalsIgnoreCase(value);
    }

    @Override
    public Integer asInteger() {
        return Integer.valueOf(value);
    }

    @Override
    public Long asLong() {
        return NumberUtil.toLong(value);
    }

    @Override
    public Float asFloat() {
        return Float.valueOf(value);
    }

    @Override
    public Double asDouble() {
        return Double.valueOf(value);
    }

    @Override
    public Date asDate() {
        return DateFormatUtil.stringToDate(asString());
    }

    @Override
    public Timestamp asTimestamp() {
        try {
            if (NumberUtil.isCreatable(value)) {
                return new Timestamp(asLong());
            }

            return DateFormatUtil.stringToTimestamp(value);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Object asTime() {
        return asString();
    }

    @Override
    public byte[] asByteArray() {
        return StringUtil.hexStringToByteArray(value.substring(2));
    }
}
