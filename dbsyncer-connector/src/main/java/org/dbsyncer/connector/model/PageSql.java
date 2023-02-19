package org.dbsyncer.connector.model;

import org.dbsyncer.connector.config.SqlBuilderConfig;

import java.util.Set;

public class PageSql {

    private SqlBuilderConfig sqlBuilderConfig;

    private String querySql;

    private String quotation;

    private Set<String> primaryKeys;

    public PageSql(String querySql, String quotation, Set<String> primaryKeys) {
        this.querySql = querySql;
        this.quotation = quotation;
        this.primaryKeys = primaryKeys;
    }

    public PageSql(SqlBuilderConfig sqlBuilderConfig, String querySql, Set<String> primaryKeys) {
        this.sqlBuilderConfig = sqlBuilderConfig;
        this.querySql = querySql;
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

    public Set<String> getPrimaryKeys() {
        return primaryKeys;
    }
}