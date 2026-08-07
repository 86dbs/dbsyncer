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

    protected Map<String, Expression> columnMap = new HashMap<>();
    protected List<Field> fields;

    public void findColumn(Expression expression) {
        if (expression instanceof IsNullExpression) {
            IsNullExpression isNullExpression = (IsNullExpression) expression;
            Column column = (Column) isNullExpression.getLeftExpression();
            columnMap.put(StringUtil.replace(column.getColumnName(), StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY), expression);
            return;
        }

        BinaryExpression binaryExpression = (BinaryExpression) expression;
        if (binaryExpression.getLeftExpression() instanceof Column) {
            Column column = (Column) binaryExpression.getLeftExpression();
            columnMap.put(StringUtil.replace(column.getColumnName(), StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY), binaryExpression.getRightExpression());
            return;
        }
        findColumn(binaryExpression.getLeftExpression());
        findColumn(binaryExpression.getRightExpression());
    }

    public List<Object> columnMapToData() {
        List<Object> data = new LinkedList<>();
        // 需要进行数据库类型
        for (Field field : fields) {
            if (!columnMap.containsKey(field.getName())) {
                // INSERT 缺列→null；UPDATE 缺列→哨兵（由子类 missingColumnValue 区分）
                data.add(missingColumnValue());
                continue;
            }
            OracleColumnValue oracleColumnValue = new OracleColumnValue(columnMap.get(field.getName()));
            if (oracleColumnValue.isNull()) {
                data.add(null);
                continue;
            }
            switch (field.getType()) {
                case Types.NUMERIC:
                case Types.DECIMAL:
                    data.add(oracleColumnValue.asBigDecimal());
                    break;
                case Types.TIME:
                case Types.TIMESTAMP:
                    data.add(oracleColumnValue.asTimestamp());
                    break;
                // timezone
                case OracleTypes.TIMESTAMPTZ:
                    data.add(oracleColumnValue.asOffsetDateTime());
                    break;
                default:
                    data.add(oracleColumnValue.asString());
            }
        }
        return data;
    }

    /**
     * redo 中未出现的列的占位值。UPDATE 需覆盖为 {@link OracleUnchangedValue}。
     */
    protected Object missingColumnValue() {
        return null;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

}