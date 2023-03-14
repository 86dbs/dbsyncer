package org.dbsyncer.connector.model;

public class SqlTable {

    private String sqlName;

    private String sql;

    private String table;

    public SqlTable() {
    }

    public SqlTable(String sqlName, String sql, String table) {
        this.sqlName = sqlName;
        this.sql = sql;
        this.table = table;
    }

    public String getSqlName() {
        return sqlName;
    }

    public void setSqlName(String sqlName) {
        this.sqlName = sqlName;
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
