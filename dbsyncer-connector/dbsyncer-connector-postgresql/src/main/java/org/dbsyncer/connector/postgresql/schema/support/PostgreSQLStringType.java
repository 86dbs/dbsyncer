package org.dbsyncer.connector.postgresql.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * PostgreSQL字符串类型支持
 */
public final class PostgreSQLStringType extends StringType {
    private enum TypeEnum {
        VARCHAR, // 可变长度字符串
        CHAR,    // 固定长度字符串
        BPCHAR   // 固定长度空白填充字符串
        // 移除了text、json、jsonb、xml、user-defined，因为它们有专门的DataType实现类
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.STRING;
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            return (String) val;
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return val;
        }
        return super.convert(val, field);
    }
    
    @Override
    public Field handleDDLParameters(ColDataType colDataType) {
        Field result = new Field();
        
        // 处理字符串类型，根据参数设置columnSize
        List<String> argsList = colDataType.getArgumentsStringList();
        if (argsList != null && !argsList.isEmpty() && argsList.size() >= 1) {
            try {
                int size = Integer.parseInt(argsList.get(0));
                result.setColumnSize(size);
            } catch (NumberFormatException e) {
                // 忽略解析错误，使用默认值
            }
        }
        
        return result;
    }
}