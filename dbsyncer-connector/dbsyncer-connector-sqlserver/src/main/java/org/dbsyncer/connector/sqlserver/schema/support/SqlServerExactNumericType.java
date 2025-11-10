/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.LongType;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SQL Server 精确数值类型
 * 包括整数类型和精确小数类型
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class SqlServerExactNumericType extends LongType {

    @Override
    public Set<String> getSupportedTypeName() {
        // 注意：TIMESTAMP是SQL Server的行版本控制类型，是8字节二进制数据，可以转换为long
        // TIMESTAMP本质上是一个递增的版本号，用long表示更符合语义
        return new HashSet<>(Arrays.asList("BIT", "TINYINT", "SMALLINT", "INT", "BIGINT", "INT IDENTITY", "BIGINT IDENTITY", "SMALLINT IDENTITY", "TINYINT IDENTITY", "TIMESTAMP"));
    }

    @Override
    protected Long merge(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).longValue();
        }
        if (val instanceof Boolean) {
            return ((Boolean) val) ? 1L : 0L;
        }
        if (val instanceof byte[]) {
            // SQL Server TIMESTAMP是8字节二进制数据，可以转换为long
            // SQL Server使用小端序（little-endian）存储
            byte[] bytes = (byte[]) val;
            if (bytes.length == 8) {
                // 使用ByteBuffer处理字节序，SQL Server使用小端序
                ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
                return buffer.getLong();
            } else if (bytes.length > 0) {
                // 如果不是8字节，补齐或截断为8字节
                byte[] paddedBytes = new byte[8];
                int len = Math.min(bytes.length, 8);
                System.arraycopy(bytes, 0, paddedBytes, 0, len);
                ByteBuffer buffer = ByteBuffer.wrap(paddedBytes).order(ByteOrder.LITTLE_ENDIAN);
                return buffer.getLong();
            }
            return 0L;
        }
        if (val instanceof String) {
            try {
                return Long.parseLong((String) val);
            } catch (NumberFormatException e) {
                return 0L;
            }
        }
        return 0L;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Long) {
            return val;
        }
        if (val instanceof Integer) {
            return ((Integer) val).longValue();
        }
        if (val instanceof Boolean) {
            return ((Boolean) val) ? 1L : 0L;
        }
        if (val instanceof BigDecimal) {
            return ((BigDecimal) val).longValue();
        }
        if (val instanceof byte[]) {
            // SQL Server TIMESTAMP：将8字节二进制转换为long
            // SQL Server使用小端序（little-endian）存储
            byte[] bytes = (byte[]) val;
            if (bytes.length == 8) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
                return buffer.getLong();
            } else if (bytes.length > 0) {
                byte[] paddedBytes = new byte[8];
                int len = Math.min(bytes.length, 8);
                System.arraycopy(bytes, 0, paddedBytes, 0, len);
                ByteBuffer buffer = ByteBuffer.wrap(paddedBytes).order(ByteOrder.LITTLE_ENDIAN);
                return buffer.getLong();
            }
            return 0L;
        }
        return super.convert(val, field);
    }
}