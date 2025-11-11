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
 */
public final class SqlServerBinaryType extends BytesType {

    @Override
    public Set<String> getSupportedTypeName() {
        // BYTES类型：用于小容量二进制数据，只支持BINARY和VARBINARY
        // IMAGE类型（大容量二进制）由SqlServerBlobType处理
        return new HashSet<>(Arrays.asList("BINARY", "VARBINARY"));
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
        
        String typeName = colDataType.getDataType().toUpperCase();
        java.util.List<String> argsList = colDataType.getArgumentsStringList();
        
        if ("VARBINARY".equals(typeName)) {
            // 处理 VARBINARY(n) 的长度参数（MAX 由 SchemaResolver 处理，这里不需要处理）
            if (argsList != null && !argsList.isEmpty()) {
                String arg = argsList.get(0).trim().toUpperCase();
                if (!"MAX".equals(arg)) { // Only process if not MAX
                    // 解析长度参数
                    try {
                        long size = Long.parseLong(arg);
                        result.setColumnSize(size);
                    } catch (NumberFormatException e) {
                        // 忽略解析错误，使用默认值
                    }
                }
            }
        } else if ("BINARY".equals(typeName)) {
            // 处理 BINARY 类型的长度参数
            if (argsList != null && !argsList.isEmpty()) {
                try {
                    long size = Long.parseLong(argsList.get(0).trim());
                    result.setColumnSize(size);
                } catch (NumberFormatException e) {
                    // 忽略解析错误，使用默认值
                }
            }
        }
        
        return result;
    }

}