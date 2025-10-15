/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BytesType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQL Server 二进制字符串类型
 * 包括二进制数据类型
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class SqlServerBinaryStringType extends BytesType {

    private enum TypeEnum {
        BINARY,         // 固定长度二进制
        VARBINARY,      // 可变长度二进制
        IMAGE           // 图像类型 (已弃用，建议使用 VARBINARY(MAX))
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        if (val instanceof byte[]) {
            return (byte[]) val;
        }
        if (val instanceof String) {
            return ((String) val).getBytes();
        }
        return new byte[0];
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof byte[]) {
            return val;
        }
        if (val instanceof String) {
            return ((String) val).getBytes();
        }
        return super.convert(val, field);
    }
}
