package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BooleanType;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SQL Server BOOLEAN类型：用于布尔值
 * 支持 BIT 类型
 */
public final class SqlServerBooleanType extends BooleanType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("BIT"));
    }

    @Override
    protected Boolean merge(Object val, Field field) {
        if (val instanceof Boolean) {
            return (Boolean) val;
        }
        if (val instanceof Number) {
            return convertNumberToBoolean((Number) val);
        }
        if (val instanceof byte[]) {
            return convertByteArrayToBoolean((byte[]) val);
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        // convert 和 merge 逻辑相同，直接调用 merge（Boolean 是 Object 的子类）
        return merge(val, field);
    }

    /**
     * 将 Number 类型转换为 Boolean
     * 1 为 true，其他为 false
     */
    private Boolean convertNumberToBoolean(Number num) {
        return num.intValue() == 1 ? Boolean.TRUE : Boolean.FALSE;
    }

    /**
     * 将 byte[] 类型转换为 Boolean
     * 支持从异常转储中反序列化的 byte[] 类型（Boolean 被序列化为 2 字节的 short）
     */
    private Boolean convertByteArrayToBoolean(byte[] bytes) {
        if (bytes.length >= 2) {
            // 按照 BinlogMessageUtil 的序列化格式：Boolean 被序列化为 2 字节的 short
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            short value = buffer.getShort();
            return value == 1 ? Boolean.TRUE : Boolean.FALSE;
        } else if (bytes.length == 1) {
            // 单字节格式：1 为 true，0 为 false
            return bytes[0] == 1 ? Boolean.TRUE : Boolean.FALSE;
        }
        return Boolean.FALSE;
    }
}

