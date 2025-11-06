package org.dbsyncer.connector.mysql.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.SetType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * MySQL SET类型支持
 */
public final class MySQLSetType extends SetType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("SET"));
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
        
        // 处理SET类型，根据集合值列表计算最大长度
        // SET类型可以存储多个值的组合（逗号分隔），所以长度应该是所有值长度之和加上分隔符长度
        List<String> argsList = colDataType.getArgumentsStringList();
        if (argsList != null && !argsList.isEmpty()) {
            int totalLength = argsList.stream()
                    .mapToInt(value -> {
                        // 移除可能的引号（单引号或双引号）
                        String cleanValue = value.trim();
                        if ((cleanValue.startsWith("'") && cleanValue.endsWith("'")) ||
                            (cleanValue.startsWith("\"") && cleanValue.endsWith("\""))) {
                            cleanValue = cleanValue.substring(1, cleanValue.length() - 1);
                        }
                        return cleanValue.length();
                    })
                    .sum();
            
            // 加上分隔符长度：如果有N个值，最多需要N-1个逗号
            // 但考虑到实际使用场景，SET类型通常不会选择所有值，这里采用更保守的估算
            // 假设最多选择一半的值，或者使用所有值之和加上分隔符
            int separatorLength = argsList.size() > 1 ? (argsList.size() - 1) : 0;
            int maxLength = totalLength + separatorLength;
            
            // 如果计算出的长度太大（超过VARCHAR最大限制），使用合理的上限
            // MySQL VARCHAR最大长度是65535，但为了跨数据库兼容，建议使用更小的值
            // 这里使用65535作为上限，但实际应用中可以根据目标数据库调整
            if (maxLength > 65535) {
                maxLength = 65535;
            }
            
            result.setColumnSize(maxLength);
        } else {
            // 如果没有参数，使用默认长度
            result.setColumnSize(255);
        }
        
        return result;
    }
}