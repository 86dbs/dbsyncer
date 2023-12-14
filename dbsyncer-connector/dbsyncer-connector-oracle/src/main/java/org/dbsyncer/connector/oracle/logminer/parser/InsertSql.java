package org.dbsyncer.connector.oracle.logminer.parser;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.model.Field;

public class InsertSql implements Parser{

    Insert insert;

    Map<String,String> cloumnMap;

    List<Field> columns;

    DatabaseConnectorInstance instance;

    public InsertSql(Insert insert, List<Field> columns,
            DatabaseConnectorInstance instance) {
        this.insert = insert;
        this.columns = columns;
        this.instance = instance;
        cloumnMap = new HashMap<>();
    }

    @Override
    public List<Object> parseSql() {
        List<Column> columns= insert.getColumns();
        ExpressionList values = insert.getSelect().getValues().getExpressions();
        for (int i = 0; i < columns.size(); i++) {
            cloumnMap.put(StringUtil.replace(columns.get(i).getColumnName(), StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY),values.get(i).toString());
        }
        String toSqlString = toSql(StringUtil.replace(insert.getTable().getName(), StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY));
        List<Object> data= ToResult(toSqlString);
        return data;
    }

    private String toSql(String tableName){
        StringBuilder sql = new StringBuilder("SELECT *");
        sql.append(" FROM ").append(tableName).append(" WHERE ");
        int pkCount = 0;
        for (Field field: columns) {
            if (field.isPk()){
                String value = cloumnMap.get(field.getName());
                if (StringUtil.isNotBlank(value)){
                    if (pkCount > 0){
                        sql.append(" AND ");
                    }
                    sql.append(field.getName()).append("=").append(value);
                    pkCount ++;
                }
            }
        }
        return sql.toString();
    }

    //从主键解析出来的map装载成sql并运行sql找出对应的数据
    private List<Object> ToResult(String sql){
        List<Map<String,Object>> results = instance.execute(databaseTemplate -> databaseTemplate.queryForList(sql));
        List<Object> list = new LinkedList<>();
        if (!CollectionUtils.isEmpty(results)){
            results.forEach(map->{
                for (String key:map.keySet()) {
                    list.add(map.get(key));
                }
            });
            return list;
        }
        return Collections.EMPTY_LIST;
    }
}
