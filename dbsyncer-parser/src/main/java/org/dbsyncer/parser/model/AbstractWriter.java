package org.dbsyncer.parser.model;

import org.dbsyncer.sdk.enums.ChangedEventTypeEnum;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/4 23:02
 */
public abstract class AbstractWriter {

    private ChangedEventTypeEnum typeEnum;

    private String tableGroupId;

    private String event;

    private String sql;

    public ChangedEventTypeEnum getTypeEnum() {
        return typeEnum;
    }

    public void setTypeEnum(ChangedEventTypeEnum typeEnum) {
        this.typeEnum = typeEnum;
    }

    public String getTableGroupId() {
        return tableGroupId;
    }

    public void setTableGroupId(String tableGroupId) {
        this.tableGroupId = tableGroupId;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

}