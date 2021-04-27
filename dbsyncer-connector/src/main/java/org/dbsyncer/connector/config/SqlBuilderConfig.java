package org.dbsyncer.connector.config;

import org.dbsyncer.connector.database.Database;

import java.util.List;

public class SqlBuilderConfig {

    private Database    database;
    // 表名
    private String      tableName;
    // 主键
    private String      pk;
    // 字段
    private List<Field> fields;
    // 过滤条件
    private String      queryFilter;
    // 引号
    private String      quotation;

    public SqlBuilderConfig(Database database, String tableName, String pk, List<Field> fields, String queryFilter,
                            String quotation) {
        this.database = database;
        this.tableName = tableName;
        this.pk = pk;
        this.fields = fields;
        this.queryFilter = queryFilter;
        this.quotation = quotation;
    }

    public Database getDatabase() {
        return database;
    }

    public String getTableName() {
        return tableName;
    }

    public String getPk() {
        return pk;
    }

    public List<Field> getFields() {
        return fields;
    }

    public String getQueryFilter() {
        return queryFilter;
    }

    public String getQuotation() {
        return quotation;
    }
}