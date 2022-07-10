package org.dbsyncer.parser.model;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/4 23:02
 */
public abstract class AbstractWriter {

    private String tableGroupId;

    private String event;

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
}