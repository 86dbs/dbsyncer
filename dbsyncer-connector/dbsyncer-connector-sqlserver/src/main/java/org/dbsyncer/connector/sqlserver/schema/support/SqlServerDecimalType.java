package org.dbsyncer.connector.sqlserver.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DecimalType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQL Server Decimal类型支持
 * 专门处理DECIMAL和NUMERIC类型，保持精度和小数位数
 */
public final class SqlServerDecimalType extends DecimalType {

    private enum TypeEnum {
        DECIMAL,       // 精确小数
        NUMERIC,       // 数值类型 (DECIMAL 的同义词)
        DECIMAL_IDENTITY("DECIMAL IDENTITY"),  // DECIMAL IDENTITY 自增列
        NUMERIC_IDENTITY("NUMERIC IDENTITY"),   // NUMERIC IDENTITY 自增列
        // 货币类型
        MONEY,         // 货币类型 (8字节)
        SMALLMONEY;    // 小货币类型 (4字节)

        private final String typeName;
        
        TypeEnum() {
            this.typeName = name();
        }
        
        TypeEnum(String typeName) {
            this.typeName = typeName;
        }
        
        public String getTypeName() {
            return typeName;
        }
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getTypeName).collect(Collectors.toSet());
    }

    @Override
    protected BigDecimal merge(Object val, Field field) {
        if (val instanceof BigDecimal) {
            return (BigDecimal) val;
        }
        if (val instanceof Number) {
            return new BigDecimal(val.toString());
        }
        if (val instanceof String) {
            try {
                return new BigDecimal((String) val);
            } catch (NumberFormatException e) {
                return BigDecimal.ZERO;
            }
        }
        if (val instanceof Boolean) {
            return ((Boolean) val) ? BigDecimal.ONE : BigDecimal.ZERO;
        }
        return BigDecimal.ZERO;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof BigDecimal) {
            return val;
        }
        if (val instanceof Number) {
            return new BigDecimal(val.toString());
        }
        if (val instanceof String) {
            try {
                return new BigDecimal((String) val);
            } catch (NumberFormatException e) {
                return BigDecimal.ZERO;
            }
        }
        if (val instanceof Boolean) {
            return ((Boolean) val) ? BigDecimal.ONE : BigDecimal.ZERO;
        }
        return super.convert(val, field);
    }
    
    @Override
    public Field handleDDLParameters(ColDataType colDataType) {
        Field result = new Field();
        
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
                    // 忽略解析错误
                }
            }
        }
        
        return result;
    }
}