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
        byte[] bytes = asByteArray();
        int expectedLength = BinlogByteEnum.SHORT.getByteLength();
        
        // 如果字节数组长度小于期望长度，进行填充
        if (bytes.length < expectedLength) {
            byte[] paddedBytes = new byte[expectedLength];
            System.arraycopy(bytes, 0, paddedBytes, 0, bytes.length);
            // 剩余字节填充为0
            ByteBuffer buffer = ByteBuffer.allocate(expectedLength);
            buffer.put(paddedBytes);
            buffer.flip();
            return buffer.asShortBuffer().get();
        }
        
        // 如果字节数组长度大于等于期望长度，只取前 expectedLength 字节
        ByteBuffer buffer = ByteBuffer.allocate(expectedLength);
        buffer.put(bytes, 0, expectedLength);
        buffer.flip();
        return buffer.asShortBuffer().get();
    }

    @Override
    public Integer asInteger() {
        byte[] bytes = asByteArray();
        if (bytes.length == BinlogByteEnum.BYTE.getByteLength()) {
            return NumberUtil.toInt(asString());
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
        byte[] bytes = asByteArray();
        int expectedLength = BinlogByteEnum.FLOAT.getByteLength();
        
        // 如果字节数组长度小于期望长度，进行填充
        if (bytes.length < expectedLength) {
            byte[] paddedBytes = new byte[expectedLength];
            System.arraycopy(bytes, 0, paddedBytes, 0, bytes.length);
            // 剩余字节填充为0
            ByteBuffer buffer = ByteBuffer.allocate(expectedLength);
            buffer.put(paddedBytes);
            buffer.flip();
            return buffer.asFloatBuffer().get();
        }
        
        // 如果字节数组长度大于等于期望长度，只取前 expectedLength 字节
        ByteBuffer buffer = ByteBuffer.allocate(expectedLength);
        buffer.put(bytes, 0, expectedLength);
        buffer.flip();
        return buffer.asFloatBuffer().get();
    }

    @Override
    public Double asDouble() {
        byte[] bytes = asByteArray();
        int expectedLength = BinlogByteEnum.DOUBLE.getByteLength();
        
        // 如果字节数组长度小于期望长度，进行填充
        if (bytes.length < expectedLength) {
            byte[] paddedBytes = new byte[expectedLength];
            System.arraycopy(bytes, 0, paddedBytes, 0, bytes.length);
            // 剩余字节填充为0
            ByteBuffer buffer = ByteBuffer.allocate(expectedLength);
            buffer.put(paddedBytes);
            buffer.flip();
            return buffer.asDoubleBuffer().get();
        }
        
        // 如果字节数组长度大于等于期望长度，只取前 expectedLength 字节
        ByteBuffer buffer = ByteBuffer.allocate(expectedLength);
        buffer.put(bytes, 0, expectedLength);
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