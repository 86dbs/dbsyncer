/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.deserializer;

import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public final class DatetimeV2Deserialize {

    private static final Logger logger = LoggerFactory.getLogger(DatetimeV2Deserialize.class);

    public Serializable deserializeDatetimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
        byte[] bytes = inputStream.read(5);
        long datetime = bigEndianLong(bytes, 0, 5);
        int yearMonth = bitSlice(datetime, 1, 17, 40);
        int fsp = deserializeFractionalSeconds(meta, inputStream);

        // 检查是否是 MySQL 的零值日期 0000-00-00
        // 零值日期的编码：最高位为符号位(1)，其余位为0，即 0x8000000000
        // 或者 yearMonth == 0 且所有其他字段都是 0
        if (yearMonth == 0) {
            int day = bitSlice(datetime, 18, 5, 40);
            int hour = bitSlice(datetime, 23, 5, 40);
            int minute = bitSlice(datetime, 28, 6, 40);
            int second = bitSlice(datetime, 34, 6, 40);
            // 如果所有字段都是 0，说明是零值日期，返回 null
            if (day == 0 && hour == 0 && minute == 0 && second == 0 && fsp == 0) {
                logger.warn("MySQL zero date (0000-00-00) detected, returning null. bytes={}", bytesToHex(bytes));
                return null;
            }
        }

        // MySQL binlog DATETIME2 编码方式：yearMonth = (year * 13) + month
        // 解码：year = yearMonth / 13, month = yearMonth % 13
        // 注意：yearMonth % 13 的范围是 0-12，但月份应该是 1-12
        // 如果 yearMonth % 13 == 0，说明 yearMonth 是 year * 13，这不应该出现（除非是零值日期，已在上面处理）
        int year = yearMonth / 13;
        int month = yearMonth % 13;
        int day = bitSlice(datetime, 18, 5, 40);
        int hour = bitSlice(datetime, 23, 5, 40);
        int minute = bitSlice(datetime, 28, 6, 40);
        int second = bitSlice(datetime, 34, 6, 40);

        // 输出调试信息（仅在出现异常值时输出）
        if (month == 0 || month > 12) {
            logger.error("DatetimeV2Deserialize DEBUG - Invalid month detected: bytes={}, datetime={}, yearMonth={}, year={}, month={}, day={}, hour={}, minute={}, second={}, fsp={}",
                    bytesToHex(bytes), datetime, yearMonth, year, month, day, hour, minute, second, fsp);
        }

        LocalDateTime time = LocalDateTime.of(
                year,
                month,
                day,
                hour,
                minute,
                second,
                fsp / 1000
        );
        return Timestamp.valueOf(time);
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString().trim();
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