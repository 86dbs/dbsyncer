package org.dbsyncer.connector.postgresql.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DecimalType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PostgreSQLDecimalType extends DecimalType {
    private enum TypeEnum {
        NUMERIC("numeric");

        private final String value;

        TypeEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getValue).collect(Collectors.toSet());
    }

    @Override
    protected BigDecimal merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }
    
    @Override
    public Field handleDDLParameters(ColDataType colDataType) {
        Field result = new Field();
        
        // 处理NUMERIC类型，根据参数设置columnSize和ratio
        List<String> argsList = colDataType.getArgumentsStringList();
        if (argsList != null && !argsList.isEmpty()) {
            if (argsList.size() >= 1) {
                try {
                    int size = Integer.parseInt(argsList.get(0));
                    result.setColumnSize(size);
                } catch (NumberFormatException e) {
                    // 忽略解析错误，使用默认值
                }
            }
            if (argsList.size() >= 2) {
                try {
                    int ratio = Integer.parseInt(argsList.get(1));
                    result.setRatio(ratio);
                } catch (NumberFormatException e) {
                    // 忽略解析错误
                }
            }
        }
        
        return result;
    }
}