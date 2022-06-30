package org.dbsyncer.storage.binlog.impl;

import com.google.protobuf.ByteString;
import org.dbsyncer.common.column.AbstractColumnValue;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/30 22:39
 */
public class BinlogColumnValue extends AbstractColumnValue<ByteString> {

    @Override
    public String asString() {
        return getValue().toStringUtf8();
    }

    @Override
    public byte[] asByteArray() {
        return new byte[0];
    }

    @Override
    public Short asShort() {
        return null;
    }

    @Override
    public Integer asInteger() {
        return null;
    }

    @Override
    public Long asLong() {
        return null;
    }

    @Override
    public Float asFloat() {
        return null;
    }

    @Override
    public Double asDouble() {
        return null;
    }

    @Override
    public Boolean asBoolean() {
        return null;
    }

    @Override
    public BigDecimal asDecimal() {
        return null;
    }

    @Override
    public Date asDate() {
        return null;
    }

    @Override
    public Timestamp asTimestamp() {
        return null;
    }

    @Override
    public Time asTime() {
        return Time.valueOf(getValue().toStringUtf8());
    }
}