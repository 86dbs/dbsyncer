package org.dbsyncer.connector.sqlserver.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BlobType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SQL Server BLOB类型：用于大容量二进制数据
 * 支持 IMAGE 类型（已废弃，建议使用 VARBINARY(MAX)，但为了兼容性仍支持）
 */
public final class SqlServerBlobType extends BlobType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("IMAGE"));
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

