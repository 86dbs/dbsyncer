/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.deserializer;

import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public final class DatetimeV2Deserialize {

    public Serializable deserializeDatetimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
        long datetime = bigEndianLong(inputStream.read(5), 0, 5);
        int yearMonth = bitSlice(datetime, 1, 17, 40);
        int fsp = deserializeFractionalSeconds(meta, inputStream);

        // 提取日期时间各部分
        int year = yearMonth / 13;
        int month = yearMonth % 13;
        int day = bitSlice(datetime, 18, 5, 40);
        int hour = bitSlice(datetime, 23, 5, 40);
        int minute = bitSlice(datetime, 28, 6, 40);
        int second = bitSlice(datetime, 34, 6, 40);
        int nano = fsp * 1000;

        // 检查是否为 MySQL 的零日期 (0000-00-00 00:00:00)
        // MySQL 从5.7开始默认不允许零日期 Java LocalDateTime 不支持
        // 零日期的特征：year=0 或 month=0 或 day=0
        if (year == 0 || month == 0 || day == 0) {
            // 返回 null 表示零日期，避免抛出异常
            return null;
        }

        // 额外的安全检查：确保日期时间值在有效范围内
        if (month < 1 || day < 1 || day > 31 || hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
            // 日期时间值超出有效范围，返回 null
            return null;
        }
        return toTimestamp(year, month, day, hour, minute, second, nano);
    }

    /**
     * 解析旧版 DATETIME 二进制格式（非 DATETIME2），按字面量读取，避免按 GMT 转 Unix 时间戳产生时区偏差。
     */
    public Serializable deserializeDatetime(ByteArrayInputStream inputStream) throws IOException {
        int[] parts = split(inputStream.readLong(8), 100, 6);
        int year = parts[5];
        int month = parts[4];
        int day = parts[3];
        int hour = parts[2];
        int minute = parts[1];
        int second = parts[0];
        if (year == 0 || month == 0 || day == 0) {
            return null;
        }
        if (month < 1 || day < 1 || day > 31 || hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
            return null;
        }
        return toTimestamp(year, month, day, hour, minute, second, 0);
    }

    private Timestamp toTimestamp(int year, int month, int day, int hour, int minute, int second, int nano) {
        LocalDateTime time = LocalDateTime.of(year, month, day, hour, minute, second, nano);
        return Timestamp.valueOf(time);
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
