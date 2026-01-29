/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.file.column;

import org.dbsyncer.common.column.AbstractColumnValue;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-05 23:19
 */
public class FileColumnValue extends AbstractColumnValue<String> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

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
    public BigDecimal asBigDecimal() {
        String value = getValue();
        if (isEmpty(value)) {
            return null;
        }
        try {
            return new BigDecimal(value);
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse BigDecimal from value: {}", value, e);
            return null;
        }
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
    public Time asTime() {
        String value = getValue();
        if (isEmpty(value)) {
            return null;
        }
        try {
            return Time.valueOf(value);
        } catch (IllegalArgumentException e) {
            logger.warn("Failed to parse Time from value: {}", value, e);
            return null;
        }
    }

    @Override
    public byte[] asByteArray() {
        return StringUtil.hexStringToByteArray(value.substring(2));
    }

    @Override
    public Byte asByte() {
        String value = getValue();
        if (isEmpty(value)) {
            return 0;
        }
        return Byte.parseByte(value);
    }

    @Override
    public Short asShort() {
        String value = getValue();
        if (isEmpty(value)) {
            return null;
        }
        try {
            return Short.valueOf(value);
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse Short from value: {}", value, e);
            return null;
        }
    }
}
