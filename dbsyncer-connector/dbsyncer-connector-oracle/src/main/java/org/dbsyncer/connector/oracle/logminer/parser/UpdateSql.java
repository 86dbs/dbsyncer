package org.dbsyncer.connector.oracle.logminer.parser;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.update.Update;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.model.Field;

public class UpdateSql implements Parser {

    Update update;

    Map<String, String> cloumnMap;

    List<Field> columns;

    DatabaseConnectorInstance instance;

    public UpdateSql(Update update, List<Field> columns,
            DatabaseConnectorInstance instance) {
        this.update = update;
        this.columns = columns;
        this.instance = instance;
        cloumnMap = new HashMap<>();
    }

    @Override
    public List<Object> parseSql() {
        findColumn(update.getWhere());
        String toSqlString = toSql(
                StringUtil.replace(update.getTable().getName(), StringUtil.DOUBLE_QUOTATION,
                        StringUtil.EMPTY));
        List<Object> data = ToResult(toSqlString);
        return data;
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

    private String toSql(String tableName) {
        StringBuilder sql = new StringBuilder("SELECT *");
        sql.append(" FROM ").append(tableName).append(" WHERE ");
        int pkCount = 0;
        for (Field field : columns) {
            if (field.isPk()) {
                String value = cloumnMap.get(field.getName());
                if (StringUtil.isNotBlank(value)) {
                    if (pkCount > 0) {
                        sql.append(" AND ");
                    }
                    sql.append(field.getName()).append("=").append(value);
                    pkCount++;
                }
            }
        }
        return sql.toString();
    }

    //从主键解析出来的map装载成sql并运行sql找出对应的数据
    private List<Object> ToResult(String sql) {
        List<Map<String, Object>> results = instance.execute(
                databaseTemplate -> databaseTemplate.queryForList(sql));
        List<Object> list = new LinkedList<>();
        if (!CollectionUtils.isEmpty(results)) {
            results.forEach(map -> {
                for (String key : map.keySet()) {
                    list.add(map.get(key));
                }
            });
            return list;
        }
        return Collections.EMPTY_LIST;
    }
}
