package org.dbsyncer.parser.model;

import org.dbsyncer.sdk.enums.ChangedEventTypeEnum;
import org.dbsyncer.sdk.model.ChangedOffset;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/4 23:02
 */
public abstract class AbstractWriter {

    private ChangedEventTypeEnum typeEnum;

    private ChangedOffset changedOffset;

    private String tableName;

    private String event;

    private String sql;

    private String traceId;

    public ChangedEventTypeEnum getTypeEnum() {
        return typeEnum;
    }

    public void setTypeEnum(ChangedEventTypeEnum typeEnum) {
        this.typeEnum = typeEnum;
    }

    public ChangedOffset getChangedOffset() {
        return changedOffset;
    }

    public void setChangedOffset(ChangedOffset changedOffset) {
        this.changedOffset = changedOffset;
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

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }
}