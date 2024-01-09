/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.logminer.parser;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.schema.Column;
import oracle.jdbc.OracleTypes;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-14 22:39
 */
public abstract class AbstractParser implements Parser {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    protected Map<String, Expression> columnMap = new HashMap<>();
    protected List<Field> fields;

    public void findColumn(Expression expression) {
        if (expression instanceof IsNullExpression) {
            IsNullExpression isNullExpression = (IsNullExpression) expression;
            Column column = (Column) isNullExpression.getLeftExpression();
            columnMap.put(StringUtil.replace(column.getColumnName(), StringUtil.DOUBLE_QUOTATION,
                    StringUtil.EMPTY), expression);
            return;
        }

        BinaryExpression binaryExpression = (BinaryExpression) expression;
        if (binaryExpression.getLeftExpression() instanceof Column) {
            Column column = (Column) binaryExpression.getLeftExpression();
            columnMap.put(StringUtil.replace(column.getColumnName(), StringUtil.DOUBLE_QUOTATION,
                    StringUtil.EMPTY), binaryExpression.getRightExpression());
            return;
        }
        findColumn(binaryExpression.getLeftExpression());
        findColumn(binaryExpression.getRightExpression());
    }

    public List<Object> columnMapToData() {
        List<Object> data = new LinkedList<>();
        //需要进行数据库类型
        for (Field field : fields) {
            OracleColumnValue oracleColumnValue = new OracleColumnValue(
                    columnMap.get(field.getName()));
            switch (field.getType()) {
                case Types.NUMERIC:
                    data.add(convertNumber(oracleColumnValue,field));
                    break;
                case Types.DECIMAL:
                    data.add(oracleColumnValue.asBigDecimal());
                    break;
                case Types.TIME:
                case Types.TIMESTAMP:
                    data.add(oracleColumnValue.asTimestamp());
                    break;
                //timezone
                case OracleTypes.TIMESTAMPTZ:
                    data.add(oracleColumnValue.asOffsetDateTime());
                    break;
                default:
                    data.add(oracleColumnValue.asString());
            }
        }
        return data;
    }

    private Object convertNumber(OracleColumnValue oracleColumnValue,Field field) {
        if (field.getRatio() <=0){
            int result =field.getColumnSize() - field.getRatio();
            if ( result <10){
                return oracleColumnValue.asInteger();
            }
            if (result <19){
                return oracleColumnValue.asBigInteger();
            }
            if (result <39){
                return oracleColumnValue.asBigDecimal();
            }
            return oracleColumnValue.asString();
        }else{
            return oracleColumnValue.asBigDecimal();
        }
    }


    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

}