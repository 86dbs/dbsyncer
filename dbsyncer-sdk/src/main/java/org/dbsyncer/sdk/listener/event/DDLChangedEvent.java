package org.dbsyncer.sdk.listener.event;

import org.dbsyncer.sdk.enums.ChangedEventTypeEnum;

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

    public DDLChangedEvent(String database, String sourceTableName, String event, String sql, String nextFileName, Object position) {
        setSourceTableName(sourceTableName);
        setEvent(event);
        setNextFileName(nextFileName);
        setPosition(position);
        setSql(sql);
        this.database = database;
    }

    public String getDatabase() {
        return database;
    }

    @Override
    public ChangedEventTypeEnum getType() {
        return ChangedEventTypeEnum.DDL;
    }
}
