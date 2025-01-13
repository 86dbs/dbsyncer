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
    private final String sql;

    public DDLChangedEvent(String sourceTableName, String event, String sql, String nextFileName, Object position) {
        setSourceTableName(sourceTableName);
        setEvent(event);
        setNextFileName(nextFileName);
        setPosition(position);
        this.sql = sql;
    }

    @Override
    public ChangedEventTypeEnum getType() {
        return ChangedEventTypeEnum.DDL;
    }

    @Override
    public String getSql() {
        return sql;
    }
}
