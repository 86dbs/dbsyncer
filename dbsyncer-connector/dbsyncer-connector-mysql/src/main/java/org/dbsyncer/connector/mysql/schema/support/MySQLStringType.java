package org.dbsyncer.connector.mysql.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class MySQLStringType extends StringType {

    private enum TypeEnum {
        CHAR, // 固定长度，最多255个字符
        VARCHAR; // 固定长度，最多65535个字符，64K
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof byte[]) {
            return new String((byte[]) val, StandardCharsets.UTF_8);
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