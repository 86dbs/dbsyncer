/**
 * DBSyncer Copyright 2019-2024 All Rights Reserved.
 */
package org.dbsyncer.listener.oracle.dcn;

import java.util.List;

/**
 * 监听行变更事件
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-06-15 20:00
 */
public class RowChangeEvent {

    private String tableName;
    private int event;
    private List<Object> data;

    public RowChangeEvent(String tableName, int event, List<Object> data) {
        this.tableName = tableName;
        this.event = event;
        this.data = data;
    }

    public String getTableName() {
        return tableName;
    }

    public int getEvent() {
        return event;
    }

    public List<Object> getData() {
        return data;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("RowChangeEvent{")
                .append("tableName='").append(tableName).append('\'')
                .append("event='").append(event).append('\'')
                .append(", data=").append(data)
                .append('}').toString();
    }
}