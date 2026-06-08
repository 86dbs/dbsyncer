/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.deserializer;

import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.io.Serializable;

/**
 * 解析 MySQL binlog TIME 类型，按字面量读取时分秒，避免按 Unix 时间戳构造产生时区偏差。
 * <p>
 * 位域提取与 mysql-binlog-connector-java 保持一致，不对 packed 值做符号位翻转。
 */
public final class TimeDeserialize {

    public Serializable deserializeTime(ByteArrayInputStream inputStream) throws IOException {
        int packed = inputStream.readInteger(3);
        int[] parts = split(packed, 100, 3);
        return formatTime(parts[2], parts[1], parts[0], 0);
    }

    public Serializable deserializeTimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
        long packed = bigEndianLong(inputStream.read(3), 0, 3);
        int micro = deserializeFractionalSeconds(meta, inputStream);
        int hour = bitSlice(packed, 2, 10, 24);
        int minute = bitSlice(packed, 12, 6, 24);
        int second = bitSlice(packed, 18, 6, 24);
        return formatTime(hour, minute, second, micro);
    }

    private String formatTime(int hour, int minute, int second, int micro) {
        if (minute < 0 || minute > 59 || second < 0 || second > 59) {
            return null;
        }
        if (micro > 0) {
            return String.format("%02d:%02d:%02d.%06d", hour, minute, second, micro);
        }
        return String.format("%02d:%02d:%02d", hour, minute, second);
    }

    private int[] split(long value, int divider, int length) {
        int[] result = new int[length];
        for (int i = 0; i < length - 1; i++) {
            result[i] = (int) (value % divider);
            value /= divider;
        }
        result[length - 1] = (int) value;
        return result;
    }

    private long bigEndianLong(byte[] bytes, int offset, int length) {
        long result = 0;
        for (int i = offset; i < (offset + length); i++) {
            byte b = bytes[i];
            result = (result << 8) | (b >= 0 ? (int) b : (b + 256));
        }
        return result;
    }

    private int deserializeFractionalSeconds(int meta, ByteArrayInputStream inputStream) throws IOException {
        int length = (meta + 1) / 2;
        if (length > 0) {
            int fraction = bigEndianInteger(inputStream.read(length), 0, length);
            return fraction * (int) Math.pow(100, 3 - length);
        }
        return 0;
    }

    private int bigEndianInteger(byte[] bytes, int offset, int length) {
        int result = 0;
        for (int i = offset; i < (offset + length); i++) {
            byte b = bytes[i];
            result = (result << 8) | (b >= 0 ? (int) b : (b + 256));
        }
        return result;
    }

    private int bitSlice(long value, int bitOffset, int numberOfBits, int payloadSize) {
        long result = value >> payloadSize - (bitOffset + numberOfBits);
        return (int) (result & ((1 << numberOfBits) - 1));
    }
}
