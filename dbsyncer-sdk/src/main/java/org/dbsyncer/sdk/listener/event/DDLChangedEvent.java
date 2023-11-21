package org.dbsyncer.sdk.listener.event;

/**
 * DDL变更事件
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2023-09-18 23:00
 */
public final class DDLChangedEvent extends CommonChangedEvent {

    /**
     * 变更数据库
      */
    private String database;

    /**
     * 变更SQL
     */
    private String sql;

    public DDLChangedEvent(String database, String sourceTableName, String event, String sql, String nextFileName, Object position) {
        setSourceTableName(sourceTableName);
        setEvent(event);
        setNextFileName(nextFileName);
        setPosition(position);
        this.database = database;
        this.sql = sql;
    }

    public String getDatabase() {
        return database;
    }

    public String getSql() {
        return sql;
    }
}
