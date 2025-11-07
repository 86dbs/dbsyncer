/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BytesType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SQL Server 二进制字符串类型
 * 包括二进制数据类型
 * 注意：二进制字符串类型不支持 IDENTITY 属性
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class SqlServerBinaryStringType extends BytesType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("BINARY", "VARBINARY", "IMAGE"));
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        if (val instanceof byte[]) {
            return (byte[]) val;
        }
        if (val instanceof String) {
            // 使用UTF-8编码将字符串转换为字节数组
            return ((String) val).getBytes(StandardCharsets.UTF_8);
        }
        // 对于其他类型，先转换为字符串再转换为字节数组
        return String.valueOf(val).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof byte[]) {
            return val;
        }
        if (val instanceof String) {
            return ((String) val).getBytes(StandardCharsets.UTF_8);
        }
        return super.convert(val, field);
    }
}