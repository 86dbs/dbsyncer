package org.dbsyncer.connector.mysql.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnsignedDecimalType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL 无符号精确小数类型支持
 */
public final class MySQLUnsignedDecimalType extends UnsignedDecimalType {

    private enum TypeEnum {
        DECIMAL_UNSIGNED("DECIMAL UNSIGNED");

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
        if (val instanceof Number) {
            BigDecimal bd = new BigDecimal(val.toString());
            // 确保值 >= 0
            if (bd.compareTo(BigDecimal.ZERO) < 0) {
                return BigDecimal.ZERO;
            }
            return bd;
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    public Field handleDDLParameters(ColDataType colDataType) {
        // 调用父类方法设置基础信息
        Field result = super.handleDDLParameters(colDataType);

        // 处理DECIMAL类型，根据参数设置columnSize和ratio
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
                    // 忽略解析错误，使用默认值
                }
            }
        }

        return result;
    }
}

