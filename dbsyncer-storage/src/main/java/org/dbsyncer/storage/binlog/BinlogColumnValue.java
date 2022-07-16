package org.dbsyncer.storage.binlog;

import com.google.protobuf.ByteString;
import org.dbsyncer.common.column.AbstractColumnValue;

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

    private final ByteBuffer buffer = ByteBuffer.allocate(8);

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
        buffer.clear();
        buffer.put(asByteArray(), 0, 2);
        buffer.flip();
        return buffer.asShortBuffer().get();
    }

    @Override
    public Integer asInteger() {
        buffer.clear();
        buffer.put(asByteArray(), 0, 4);
        buffer.flip();
        return buffer.asIntBuffer().get();
    }

    @Override
    public Long asLong() {
        buffer.clear();
        buffer.put(asByteArray(), 0, 8);
        buffer.flip();
        return buffer.asLongBuffer().get();
    }

    @Override
    public Float asFloat() {
        buffer.clear();
        buffer.put(asByteArray(), 0, 4);
        buffer.flip();
        return buffer.asFloatBuffer().get();
    }

    @Override
    public Double asDouble() {
        buffer.clear();
        buffer.put(asByteArray(), 0, 8);
        buffer.flip();
        return buffer.asDoubleBuffer().get();
    }

    @Override
    public Boolean asBoolean() {
        buffer.clear();
        buffer.put(asByteArray(), 0, 2);
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