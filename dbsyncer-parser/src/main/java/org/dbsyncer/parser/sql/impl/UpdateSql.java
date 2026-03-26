/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.sql.impl;

import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.sql.SqlParser;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;

import java.util.List;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-10 00:45
 */
public final class UpdateSql implements SqlParser {

    private String sql;

    private String sourceTableName;

    private String targetTableName;

    private List<FieldMapping> fieldMappingList;

    public UpdateSql(String sql, String sourceTableName, String targetTableName, List<FieldMapping> fieldMappingList) {
        this.sql = sql;
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.fieldMappingList = fieldMappingList;
    }

    @Override
    public String parse() {
        try {
            Update update = (Update) CCJSqlParserUtil.parse(sql);
            // 替换表名
            Table table = new Table();
            table.setName(targetTableName);
            update.setTable(table);
            for (UpdateSet updateSet : update.getUpdateSets()) {
                List<Column> columns = updateSet.getColumns();
                for (Column column : columns) {
                    fieldMappingList.stream().filter(x->x.getSource().getName().equals(column.getColumnName().replaceAll("\"", ""))).findFirst()
                            .ifPresent(fieldMapping->column.setColumnName(fieldMapping.getTarget().getName()));
                }
            }
            whereParse(update.getWhere());
            return update.toString();
        } catch (JSQLParserException e) {
            throw new RuntimeException(e);
        }
    }

    private void whereParse(Expression expression) {
        BinaryExpression binaryExpression = (BinaryExpression) expression;
        Expression left = binaryExpression.getLeftExpression();
        Expression right = binaryExpression.getRightExpression();
        findColumn((BinaryExpression) left);
        findColumn((BinaryExpression) right);
    }

    private void findColumn(BinaryExpression binaryExpression) {
        if (binaryExpression.getLeftExpression() instanceof Column) {
            Column column = (Column) binaryExpression.getLeftExpression();
            fieldMappingList.stream().filter(x->x.getSource().getName().equals(column.getColumnName().replaceAll("\"", ""))).findFirst()
                    .ifPresent(fieldMapping->column.setColumnName(fieldMapping.getTarget().getName()));
            return;
        }
        findColumn((BinaryExpression) binaryExpression.getLeftExpression());
        findColumn((BinaryExpression) binaryExpression.getRightExpression());
    }
}