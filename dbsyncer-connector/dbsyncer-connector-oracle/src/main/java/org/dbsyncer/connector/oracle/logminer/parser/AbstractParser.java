/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.logminer.parser;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.schema.Column;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-14 22:39
 */
public abstract class AbstractParser implements Parser {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    protected Map<String, Object> columnMap = new HashMap<>();
    protected List<Field> fields;

    public void findColumn(Expression expression) {
        if (expression instanceof IsNullExpression) {
            IsNullExpression isNullExpression = (IsNullExpression) expression;
            Column column = (Column) isNullExpression.getLeftExpression();
            columnMap.put(StringUtil.replace(column.getColumnName(), StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY), null);
            return;
        }

        BinaryExpression binaryExpression = (BinaryExpression) expression;
        if (binaryExpression.getLeftExpression() instanceof Column) {
            Column column = (Column) binaryExpression.getLeftExpression();
            columnMap.put(StringUtil.replace(column.getColumnName(), StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY), parserValue(binaryExpression.getRightExpression()));
            return;
        }
        findColumn(binaryExpression.getLeftExpression());
        findColumn(binaryExpression.getRightExpression());
    }

    public Object parserValue(Expression expression) {
        if (expression instanceof NullValue) {
            return null;
        }
        // 解析sql的function，只取到关键的字符串
        if (expression instanceof Function) {
            return parseFunction((Function) expression);
        }
        if (expression instanceof StringValue) {
            StringValue val = (StringValue) expression;
            return val.getValue();
        }
        return null;
    }

    public List<Object> columnMapToData() {
        List<Object> data = new LinkedList<>();
        for (Field field : fields) {
            data.add(columnMap.get(field.getName()));
        }
        return data;
    }

    private Object parseFunction(Function function) {
        List<String> multipartName = function.getMultipartName();
        ExpressionList parameters = function.getParameters();
        if (CollectionUtils.isEmpty(multipartName) || CollectionUtils.isEmpty(parameters)) {
            return null;
        }

        String nameType = Objects.toString(multipartName.get(0));
        Object value = parameters.get(0);
        if (nameType == null || value == null) {
            return null;
        }

        if (value instanceof StringValue) {
            StringValue val = (StringValue) value;
            value = val.getValue();
        }
        try {
            switch (nameType) {
                case "TO_DATE":
                    return toDate(value);
                case "TO_TIMESTAMP":
                    return toTimestamp(value);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return value;
    }

    private Object toDate(Object value) {
        return DateFormatUtil.stringToTimestamp(Objects.toString(value));
    }

    private Object toTimestamp(Object value) {
        return DateFormatUtil.stringToTimestamp(StringUtil.replace(Objects.toString(value), StringUtil.POINT, StringUtil.EMPTY));
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

}