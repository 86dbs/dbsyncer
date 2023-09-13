package org.dbsyncer.common.event;

public class DDLChangedEvent {

    /**
     * 变更数据库
      */
    private String database;

    /**
     * 变更表名称
     */
    private String tableName;

    /**
     * 变更SQL
     */
    private String sql;

    public DDLChangedEvent(String database, String tableName, String sql) {
        this.database = database;
        this.tableName = tableName;
        this.sql = sql;
    }

    public String getDatabase() {
        return database;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSql() {
        return sql;
    }
}
