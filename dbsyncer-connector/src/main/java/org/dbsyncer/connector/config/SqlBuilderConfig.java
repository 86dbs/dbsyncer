package org.dbsyncer.connector.config;

import org.dbsyncer.connector.database.Database;

import java.util.List;

public class SqlBuilderConfig {

    private Database     database;
    // 表名
    private String       tableName;
    // 主键
    private String       pk;
    // 字段名称
    private List<String> filedNames;
    // 字段别名
    private List<String> labelNames;
    // 过滤条件
    private String       queryFilter;
    // 引号
    private String       quotation;

    public SqlBuilderConfig(Database database, String tableName, String pk, List<String> filedNames, String queryFilter,
                            String quotation) {
        this.database = database;
        this.tableName = tableName;
        this.pk = pk;
        this.filedNames = filedNames;
        this.queryFilter = queryFilter;
        this.quotation = quotation;
    }

    public SqlBuilderConfig(Database database, String tableName, String pk, List<String> filedNames,
                            List<String> labelNames, String queryFilter, String quotation) {
        this.database = database;
        this.tableName = tableName;
        this.pk = pk;
        this.filedNames = filedNames;
        this.labelNames = labelNames;
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

    public List<String> getFiledNames() {
        return filedNames;
    }

    public List<String> getLabelNames() {
        return labelNames;
    }

    public String getQueryFilter() {
        return queryFilter;
    }

    public String getQuotation() {
        return quotation;
    }
}