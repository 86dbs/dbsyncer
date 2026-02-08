/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.sql.impl;

import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.sql.SqlParser;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.insert.Insert;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.insert.Insert;

import java.util.List;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-10 00:44
 */
public final class InsertSql implements SqlParser {

    private String sql;

    private String sourceTableName;

    private String targetTableName;

    private List<FieldMapping> fieldMappingList;

    public InsertSql(String sql, String sourceTableName, String targetTableName, List<FieldMapping> fieldMappingList) {
        this.sql = sql;
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.fieldMappingList = fieldMappingList;
    }

    public String parse() {
        try {
            Insert insert = (Insert) CCJSqlParserUtil.parse(this.sql);
            // 替换表名
            Table table = new Table();
            table.setName(targetTableName);
            insert.setTable(table);
            // 替换字段
            List<Column> columns = insert.getColumns();
            for (Column column : columns) {
                fieldMappingList.stream().filter(x->x.getSource().getName().equals(column.getColumnName().replaceAll("\"", ""))).findFirst()
                        .ifPresent(fieldMapping->column.setColumnName(fieldMapping.getTarget().getName()));
            }
            return insert.toString();
        } catch (JSQLParserException e) {
            throw new RuntimeException(e);
        }
    }
}