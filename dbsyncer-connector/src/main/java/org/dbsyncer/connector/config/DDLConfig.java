package org.dbsyncer.connector.config;

public class DDLConfig {

    String tableName;

    String event;

    String targetSql;

    public DDLConfig(String tableName, String event, String targetSql) {
        this.tableName = tableName;
        this.event = event;
        this.targetSql = targetSql;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getTargetSql() {
        return targetSql;
    }

    public void setTargetSql(String targetSql) {
        this.targetSql = targetSql;
    }
}
