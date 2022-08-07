package org.dbsyncer.connector.model;

import org.dbsyncer.connector.config.SqlBuilderConfig;

public class PageSql {

    private SqlBuilderConfig sqlBuilderConfig;

    private String querySql;

    private String pk;

    public PageSql(String querySql, String pk) {
        this.querySql = querySql;
        this.pk = pk;
    }

    public PageSql(SqlBuilderConfig sqlBuilderConfig, String querySql, String pk) {
        this.sqlBuilderConfig = sqlBuilderConfig;
        this.querySql = querySql;
        this.pk = pk;
    }

    public SqlBuilderConfig getSqlBuilderConfig() {
        return sqlBuilderConfig;
    }

    public String getQuerySql() {
        return querySql;
    }

    public String getPk() {
        return pk;
    }
}