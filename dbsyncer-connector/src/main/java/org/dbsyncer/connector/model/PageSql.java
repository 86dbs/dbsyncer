package org.dbsyncer.connector.model;

import org.dbsyncer.connector.config.SqlBuilderConfig;

import java.util.List;

public class PageSql {

    private SqlBuilderConfig sqlBuilderConfig;

    private String querySql;

    private String quotation;

    private List<String> primaryKeys;

    public PageSql(SqlBuilderConfig sqlBuilderConfig, String querySql, String quotation, List<String> primaryKeys) {
        this.sqlBuilderConfig = sqlBuilderConfig;
        this.querySql = querySql;
        this.quotation = quotation;
        this.primaryKeys = primaryKeys;
    }

    public SqlBuilderConfig getSqlBuilderConfig() {
        return sqlBuilderConfig;
    }

    public String getQuerySql() {
        return querySql;
    }

    public String getQuotation() {
        return quotation;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }
}