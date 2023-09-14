package org.dbsyncer.common.event;

public class DDLChangedEvent {

    /**
     * 变更数据库
      */
    private String database;

    /**
     * 变更表名称
     */
    private String sourceTableName;

    /**
     * 变更SQL
     */
    private String sql;

    public DDLChangedEvent(String database, String sourceTableName, String sql) {
        this.database = database;
        this.sourceTableName = sourceTableName;
        this.sql = sql;
    }

    public String getDatabase() {
        return database;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public String getSql() {
        return sql;
    }
}
