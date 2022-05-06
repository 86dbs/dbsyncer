package org.dbsyncer.connector.model;

public class PageSql {

    private String querySql;

    private String pk;

    public PageSql(String querySql, String pk) {
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