package org.dbsyncer.sdk.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

import java.util.UUID;

/**
 * UUID类型基类
 * UUID/GUID是全局唯一标识符，通常以字符串形式存储和传输
 */
public abstract class UUIDType extends AbstractDataType<String> {

    protected UUIDType() {
        super(String.class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.UUID;
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            // 验证是否为有效的UUID格式
            String str = (String) val;
            try {
                UUID.fromString(str);
                return str;
            } catch (IllegalArgumentException e) {
                // 如果不是标准UUID格式，仍然返回字符串（可能是其他格式的GUID）
                return str;
            }
        }
        if (val instanceof UUID) {
            return val.toString();
        }
        if (val instanceof byte[]) {
            // 尝试将字节数组转换为UUID字符串
            byte[] bytes = (byte[]) val;
            if (bytes.length == 16) {
                // 标准UUID是16字节
                long mostSigBits = 0;
                long leastSigBits = 0;
                for (int i = 0; i < 8; i++) {
                    mostSigBits = (mostSigBits << 8) | (bytes[i] & 0xff);
                }
                for (int i = 8; i < 16; i++) {
                    leastSigBits = (leastSigBits << 8) | (bytes[i] & 0xff);
                }
                return new UUID(mostSigBits, leastSigBits).toString();
            }
            // 如果不是16字节，转换为字符串
            return new String(bytes);
        }
        // 对于其他类型，转换为字符串
        return String.valueOf(val);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return val;
        }
        if (val instanceof UUID) {
            return val.toString();
        }
        if (val instanceof byte[]) {
            byte[] bytes = (byte[]) val;
            if (bytes.length == 16) {
                long mostSigBits = 0;
                long leastSigBits = 0;
                for (int i = 0; i < 8; i++) {
                    mostSigBits = (mostSigBits << 8) | (bytes[i] & 0xff);
                }
                for (int i = 8; i < 16; i++) {
                    leastSigBits = (leastSigBits << 8) | (bytes[i] & 0xff);
                }
                return new UUID(mostSigBits, leastSigBits).toString();
            }
            return new String(bytes);
        }
        return String.valueOf(val);
    }

    @Override
    public Field handleDDLParameters(ColDataType colDataType) {
        // UUID类型通常没有长度参数，但某些数据库可能支持
        Field result = super.handleDDLParameters(colDataType);
        
        // 如果没有指定长度，默认使用36（标准UUID字符串长度：xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx）
        Long columnSize = result.getColumnSize();
        if (columnSize == null || columnSize == 0) {
            result.setColumnSize(36L);
        }
        
        return result;
    }
}

