package org.dbsyncer.connector.oracle.logminer.parser;

import java.math.BigInteger;
import java.sql.Types;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.delete.Delete;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;

/**
 * @author : life
 * @Description :
 * @Param :
 * @return :
 * @date : 2023/12/14  14:58
 */
public class DeleteSql implements Parser {

    Delete delete;

    Map<String, String> cloumnMap;

    List<Field> columns;

    public DeleteSql(Delete delete,  List<Field> columns) {
        this.delete = delete;
        this.columns = columns;
        cloumnMap = new HashMap<>();
    }

    @Override
    public List<Object> parseSql() {
        findColumn(delete.getWhere());
        return columnMapToData();
    }

    private void findColumn(Expression expression) {
        BinaryExpression binaryExpression = (BinaryExpression) expression;
        if (binaryExpression.getLeftExpression() instanceof Column) {
            Column column = (Column) binaryExpression.getLeftExpression();
            String value = binaryExpression.getRightExpression().toString();
            cloumnMap.put(StringUtil.replace(column.getColumnName(), StringUtil.DOUBLE_QUOTATION,
                    StringUtil.EMPTY), value);
            return;
        }
        findColumn(binaryExpression.getLeftExpression());
        findColumn(binaryExpression.getRightExpression());
    }

    private List<Object> columnMapToData(){
        List<Object> data = new LinkedList<>();
        for (Field field : columns) {
            if (field.isPk()) {
                Object value = cloumnMap.get(field.getName());
                switch (field.getType()) {
                    case Types.DECIMAL:
                        value = new BigInteger(
                                StringUtil.replace(value.toString(), StringUtil.SINGLE_QUOTATION,
                                        StringUtil.EMPTY));
                }
                data.add(value);
            } else {
                data.add(null);
            }
        }
        return data;
    }

}
