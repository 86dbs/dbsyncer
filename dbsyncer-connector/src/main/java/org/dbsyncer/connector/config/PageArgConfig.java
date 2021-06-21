package org.dbsyncer.connector.config;

public class PageArgConfig {

    private String querySql;

    private Object[] args;

    public PageArgConfig(String querySql, Object[] args) {
        this.querySql = querySql;
        this.args = args;
    }

    public String getQuerySql() {
        return querySql;
    }

    public Object[] getArgs() {
        return args;
    }
}