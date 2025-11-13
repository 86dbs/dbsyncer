package org.dbsyncer.sdk.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public abstract class BytesType extends AbstractDataType<byte[]> {

    protected BytesType() {
        super(byte[].class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.BYTES;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof byte[]) {
            return val;
        }
        if (val instanceof String) {
            return ((String) val).getBytes(StandardCharsets.UTF_8);
        }
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            ByteBuffer buffer = ByteBuffer.allocate(2);
            buffer.putShort((short) (b ? 1 : 0));
            return buffer.array();
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    public Field handleDDLParameters(ColDataType colDataType) {
        // 调用父类方法设置基础信息
        Field result = super.handleDDLParameters(colDataType);

        // 处理二进制类型，根据参数设置columnSize
        // 注意：MAX 等数据库特定语法由各数据库的实现类自行处理
        List<String> argsList = colDataType.getArgumentsStringList();
        if (argsList != null && !argsList.isEmpty() && argsList.size() >= 1) {
            try {
                long size = Long.parseLong(argsList.get(0).trim());
                result.setColumnSize(size);
            } catch (NumberFormatException e) {
                // 忽略解析错误（可能是数据库特定语法如MAX），由各数据库实现类处理
            }
        }

        // 根据原始类型名称判断字段长度是否固定
        // BINARY 为固定长度，VARBINARY 为可变长度
        String originalTypeName = colDataType.getDataType();
        if (originalTypeName != null) {
            Boolean isSizeFixed = determineIsSizeFixed(originalTypeName);
            result.setIsSizeFixed(isSizeFixed);
        }

        return result;
    }

    /**
     * 判断二进制类型字段长度是否固定
     * 基类不处理任何类型判断，由各数据库的实现类自行处理
     * 
     * @param typeName 原始类型名称（如 BINARY, VARBINARY 等）
     * @return true表示固定长度，false表示可变长度，null表示未设置或不适用
     */
    protected Boolean determineIsSizeFixed(String typeName) {
        // 基类不处理任何类型判断，由各数据库的实现类自行处理
        return null;
    }
}
