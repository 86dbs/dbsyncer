package org.dbsyncer.storage.binlog.impl;

import com.google.protobuf.ByteString;
import org.dbsyncer.common.column.AbstractColumnValue;
import org.dbsyncer.common.util.DateFormatUtil;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
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
        return getValue().toByteArray();
    }

    @Override
    public Short asShort() {
        final ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.put(getValue().toByteArray());
        return buffer.asShortBuffer().get();
    }

    @Override
    public Integer asInteger() {
        final ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.put(getValue().toByteArray());
        return buffer.asIntBuffer().get();
    }

    @Override
    public Long asLong() {
        final ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.put(getValue().toByteArray());
        return buffer.asLongBuffer().get();
    }

    @Override
    public Float asFloat() {
        final ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.put(getValue().toByteArray());
        return buffer.asFloatBuffer().get();
    }

    @Override
    public Double asDouble() {
        final ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.put(getValue().toByteArray());
        return buffer.asDoubleBuffer().get();
    }

    @Override
    public Boolean asBoolean() {
        final ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.put(getValue().toByteArray());
        return buffer.asShortBuffer().get() == 1;
    }

    @Override
    public BigDecimal asDecimal() {
        return new BigDecimal(asString());
    }

    @Override
    public Date asDate() {
        return DateFormatUtil.stringToDate(asString());
    }

    @Override
    public Timestamp asTimestamp() {
        return new Timestamp(asLong());
    }

    @Override
    public Time asTime() {
        return Time.valueOf(asString());
    }
}