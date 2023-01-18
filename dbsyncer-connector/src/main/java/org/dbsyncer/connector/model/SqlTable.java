package org.dbsyncer.connector.model;

public class SqlTable {

    private String sql;

    private String table;

    public SqlTable() {
    }

    public SqlTable(String sql, String table) {
        this.sql = sql;
        this.table = table;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}
