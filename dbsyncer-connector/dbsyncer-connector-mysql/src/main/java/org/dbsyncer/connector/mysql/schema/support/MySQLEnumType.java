package org.dbsyncer.connector.mysql.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.EnumType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class MySQLEnumType extends EnumType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("ENUM"));
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            return (String) val;
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    public Field handleDDLParameters(ColDataType colDataType) {
        Field result = super.handleDDLParameters(colDataType);
        
        // 处理ENUM类型，根据枚举值列表计算最大长度
        // ENUM类型只存储单个值，所以长度应该是所有枚举值中字符串长度最大的那个
        List<String> argsList = colDataType.getArgumentsStringList();
        if (argsList != null && !argsList.isEmpty()) {
            int maxLength = argsList.stream()
                    .mapToInt(value -> {
                        // 移除可能的引号（单引号或双引号）
                        String cleanValue = value.trim();
                        if ((cleanValue.startsWith("'") && cleanValue.endsWith("'")) ||
                            (cleanValue.startsWith("\"") && cleanValue.endsWith("\""))) {
                            cleanValue = cleanValue.substring(1, cleanValue.length() - 1);
                        }
                        return cleanValue.length();
                    })
                    .max()
                    .orElse(255); // 如果没有枚举值，使用默认值255
            
            result.setColumnSize(maxLength);
        } else {
            // 如果没有参数，使用默认长度
            result.setColumnSize(255);
        }
        
        return result;
    }
}