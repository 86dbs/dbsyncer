/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.model;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-23 15:27
 */
public class DataSyncEvent {

    /**
     * 事件类型 INSERT/UPDATE/DELETE {@link org.dbsyncer.sdk.constant.ConnectorConstant}
     */
    private String event;

    /**
     * 变更的行数据
     */
    private Object data;

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
