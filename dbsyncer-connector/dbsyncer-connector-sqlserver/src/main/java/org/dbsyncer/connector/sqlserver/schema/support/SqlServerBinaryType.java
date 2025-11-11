/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
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
public final class SqlServerBinaryType extends BytesType {

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

    @Override
    public Field handleDDLParameters(ColDataType colDataType) {
        // 调用父类方法设置基础信息
        Field result = super.handleDDLParameters(colDataType);
        
        // SQL Server IMAGE类型可以存储最大2GB的数据
        // 如果类型是IMAGE且没有指定长度，设置默认容量为2GB
        String typeName = colDataType.getDataType().toUpperCase();
        if ("IMAGE".equals(typeName) && result.getColumnSize() == 0L) {
            // IMAGE类型最大容量：2,147,483,647字节 (2^31-1)
            result.setColumnSize(2147483647L);
        }
        
        return result;
    }
}