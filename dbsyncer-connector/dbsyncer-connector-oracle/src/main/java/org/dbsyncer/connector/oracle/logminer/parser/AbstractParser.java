/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.logminer.parser;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.schema.Column;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-14 22:39
 */
public abstract class AbstractParser implements Parser {

    protected Map<String, String> columnMap = new HashMap<>();
    protected String tableName;
    protected List<Field> fields;

    public void findColumn(Expression expression) {
        if (expression instanceof IsNullExpression){
            IsNullExpression isNullExpression = (IsNullExpression) expression;
            Column column = (Column) isNullExpression.getLeftExpression();
            columnMap.put(StringUtil.replace(column.getColumnName(), StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY),
                    null);
            return;
        }

        BinaryExpression binaryExpression = (BinaryExpression) expression;
        if (binaryExpression.getLeftExpression() instanceof Column) {
            Column column = (Column) binaryExpression.getLeftExpression();
            columnMap.put(StringUtil.replace(column.getColumnName(), StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY),
                    parserValue(binaryExpression.getRightExpression()));
            return;
        }
        findColumn(binaryExpression.getLeftExpression());
        findColumn(binaryExpression.getRightExpression());
    }

    public String parserValue(Expression expression){
        if (expression instanceof Function){
            return parserFunction((Function) expression);
        }
        if (expression instanceof NullValue){
            return null;
        }
        return expression.toString();
    }

    //解析sql的function，只取到关键的字符串
    public String parserFunction(Function function){
        if (function.getMultipartName().get(0).equals("TO_DATE")){
            return StringUtil.replace(function.getParameters().get(0).toString(),StringUtil.SINGLE_QUOTATION,StringUtil.EMPTY);
        }
        return "";
    }


    public List<Object> columnMapToData(){
        List<Object> data = new LinkedList<>();
        for (Field field: fields) {
            Object value = OracleTypeParser.convertToJavaType(field,columnMap.get(field.getName()));
            data.add(value);
        }
        return data;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

}