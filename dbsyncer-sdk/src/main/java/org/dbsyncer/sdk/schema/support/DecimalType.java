package org.dbsyncer.sdk.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

import java.math.BigDecimal;
import java.util.List;

public abstract class DecimalType extends AbstractDataType<BigDecimal> {

    protected DecimalType() {
        super(BigDecimal.class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.DECIMAL;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof BigDecimal) {
            return val;
        }
        if (val instanceof String) {
            return new BigDecimal((String) val);
        }
        if (val instanceof Number) {
            return new BigDecimal(val.toString());
        }
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            return new BigDecimal(b ? 1 : 0);
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
