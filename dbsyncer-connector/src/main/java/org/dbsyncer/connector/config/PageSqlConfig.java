package org.dbsyncer.connector.config;

public class PageSqlConfig {

    private String querySql;

    private String pk;

    public PageSqlConfig(String querySql, String pk) {
        this.querySql = querySql;
        this.pk = pk;
    }

    public String getQuerySql() {
        return querySql;
    }

    public String getPk() {
        return pk;
    }
}