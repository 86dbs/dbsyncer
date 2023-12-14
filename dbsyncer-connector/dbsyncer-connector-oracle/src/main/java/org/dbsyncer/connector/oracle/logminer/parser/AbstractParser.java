/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.logminer.parser;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.model.Field;

import java.util.Collections;
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

    protected Map<String, String> columnMap = new HashMap<>();
    protected String tableName;
    protected List<Field> fields;
    protected DatabaseConnectorInstance instance;

    public void findColumn(Expression expression) {
        BinaryExpression binaryExpression = (BinaryExpression) expression;
        if (binaryExpression.getLeftExpression() instanceof Column) {
            Column column = (Column) binaryExpression.getLeftExpression();
            String value = binaryExpression.getRightExpression().toString();
            columnMap.put(StringUtil.replace(column.getColumnName(), StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY), value);
            return;
        }
        findColumn(binaryExpression.getLeftExpression());
        findColumn(binaryExpression.getRightExpression());
    }

    // 从主键解析出来的map装载成sql并运行sql找出对应的数据
    public List<Object> getColumnsFromDB() {
        List<Map<String, Object>> rows = instance.execute(databaseTemplate -> databaseTemplate.queryForList(getRowByPk()));
        List<Object> list = new LinkedList<>();
        if (!CollectionUtils.isEmpty(rows)) {
            rows.forEach(map -> {
                for (String key : map.keySet()) {
                    list.add(map.get(key));
                }
            });
            return list;
        }
        return Collections.EMPTY_LIST;
    }

    private String getRowByPk() {
        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(getTableName()).append(" WHERE ");
        int pkCount = 0;
        for (Field field : fields) {
            if (field.isPk()) {
                String value = columnMap.get(field.getName());
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

    public void setInstance(DatabaseConnectorInstance instance) {
        this.instance = instance;
    }
}