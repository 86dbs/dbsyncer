package org.dbsyncer.connector.config;

public class PageSqlBuilderConfig {

    private String querySql;

    private SqlBuilderConfig config;

    public PageSqlBuilderConfig(SqlBuilderConfig config, String querySql) {
        this.config = config;
        this.querySql = querySql;
    }

    public SqlBuilderConfig getConfig() {
        return config;
    }

    public String getQuerySql() {
        return querySql;
    }
}