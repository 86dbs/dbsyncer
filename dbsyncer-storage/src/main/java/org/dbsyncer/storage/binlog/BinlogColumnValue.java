/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.storage.binlog;

import com.google.protobuf.ByteString;
import org.dbsyncer.common.column.AbstractColumnValue;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.storage.enums.BinlogByteEnum;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-06-30 22:39
 */
public class BinlogColumnValue extends AbstractColumnValue<ByteString> {

    public BinlogColumnValue(ByteString v) {
        setValue(v);
    }

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
        ByteBuffer buffer = ByteBuffer.allocate(BinlogByteEnum.SHORT.getByteLength());
        buffer.put(asByteArray(), 0, buffer.capacity());
        buffer.flip();
        return buffer.asShortBuffer().get();
    }

    @Override
    public Integer asInteger() {
        byte[] bytes = asByteArray();
        if (bytes.length == BinlogByteEnum.BYTE.getByteLength()) {
            return NumberUtil.toInt(ByteString.copyFrom(bytes).toStringUtf8());
        }
        if (bytes.length == BinlogByteEnum.SHORT.getByteLength()) {
            Short aShort = asShort();
            return new Integer(aShort);
        }

        ByteBuffer buffer = ByteBuffer.allocate(BinlogByteEnum.INTEGER.getByteLength());
        buffer.put(bytes, 0, buffer.capacity());
        buffer.flip();
        return buffer.asIntBuffer().get();
    }

    @Override
    public Long asLong() {
        ByteBuffer buffer = ByteBuffer.allocate(BinlogByteEnum.LONG.getByteLength());
        buffer.put(asByteArray(), 0, buffer.capacity());
        buffer.flip();
        return buffer.asLongBuffer().get();
    }

    @Override
    public Float asFloat() {
        ByteBuffer buffer = ByteBuffer.allocate(BinlogByteEnum.FLOAT.getByteLength());
        buffer.put(asByteArray(), 0, buffer.capacity());
        buffer.flip();
        return buffer.asFloatBuffer().get();
    }

    @Override
    public Double asDouble() {
        ByteBuffer buffer = ByteBuffer.allocate(BinlogByteEnum.DOUBLE.getByteLength());
        buffer.put(asByteArray(), 0, buffer.capacity());
        buffer.flip();
        return buffer.asDoubleBuffer().get();
    }

    @Override
    public Boolean asBoolean() {
        ByteBuffer buffer = ByteBuffer.allocate(BinlogByteEnum.SHORT.getByteLength());
        buffer.put(asByteArray(), 0, buffer.capacity());
        buffer.flip();
        return buffer.asShortBuffer().get() == 1;
    }

    @Override
    public BigDecimal asBigDecimal() {
        return new BigDecimal(asString());
    }

    @Override
    public Date asDate() {
        return new Date(asLong());
    }

    @Override
    public Timestamp asTimestamp() {
        return new Timestamp(asLong());
    }

    @Override
    public Time asTime() {
        return new Time(asLong());
    }
}