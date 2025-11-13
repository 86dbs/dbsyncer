package org.dbsyncer.sdk.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public abstract class StringType extends AbstractDataType<String> {

    protected StringType() {
        super(String.class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.STRING;
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            return (String) val;
        }
        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }
        // 对于其他类型，转换为字符串
        return String.valueOf(val);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return val;
        }
        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }

        if (val instanceof Number) {
            Number number = (Number) val;
            return number.toString();
        }

        if (val instanceof LocalDateTime) {
            return ((LocalDateTime) val).format(DateFormatUtil.YYYY_MM_DD_HH_MM_SS);
        }

        if (val instanceof LocalDate) {
            return ((LocalDate) val).format(DateFormatUtil.YYYY_MM_DD);
        }

        if (val instanceof Timestamp) {
            return DateFormatUtil.timestampToString((Timestamp) val);
        }

        if (val instanceof Date) {
            return DateFormatUtil.dateToString((Date) val);
        }

        if (val instanceof java.util.Date) {
            return DateFormatUtil.dateToString((java.util.Date) val);
        }

        return throwUnsupportedException(val, field);
    }

    @Override
    public Field handleDDLParameters(ColDataType colDataType) {
        // 调用父类方法设置基础信息
        Field result = super.handleDDLParameters(colDataType);

        // 处理字符串类型，根据参数设置columnSize
        // 注意：MAX 等数据库特定语法由各数据库的实现类自行处理
        List<String> argsList = colDataType.getArgumentsStringList();
        if (argsList != null && !argsList.isEmpty() && argsList.size() >= 1) {
            try {
                int size = Integer.parseInt(argsList.get(0));
                result.setColumnSize(size);
            } catch (NumberFormatException e) {
                // 忽略解析错误（可能是数据库特定语法如MAX），由各数据库实现类处理
            }
        }

        // 根据原始类型名称判断字段长度是否固定
        // 固定长度：CHAR、NCHAR
        // 可变长度：VARCHAR、NVARCHAR、TEXT等
        String originalTypeName = colDataType.getDataType();
        if (originalTypeName != null) {
            Boolean isSizeFixed = determineIsSizeFixed(originalTypeName);
            result.setIsSizeFixed(isSizeFixed);
        }

        return result;
    }

    /**
     * 判断字符串类型字段长度是否固定
     * 基类不处理任何类型判断，由各数据库的实现类自行处理
     * 
     * @param typeName 原始类型名称（如 CHAR, VARCHAR, NCHAR, NVARCHAR 等）
     * @return true表示固定长度，false表示可变长度，null表示未设置或不适用
     */
    protected Boolean determineIsSizeFixed(String typeName) {
        // 基类不处理任何类型判断，由各数据库的实现类自行处理
        return null;
    }
}