/**
 * DBSyncer Copyright 2019-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.listener.event;

import java.util.List;

/**
 * 监听行变更事件
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-06-15 20:00
 */
public final class RowChangedEvent extends CommonChangedEvent {
    private List<Object> dataList;

    public RowChangedEvent(String sourceTableName, String event, List<Object> data) {
        this(sourceTableName, event, data, null, null);
    }

    public RowChangedEvent(String sourceTableName, String event, List<Object> data, String nextFileName, Object position) {
        setSourceTableName(sourceTableName);
        setEvent(event);
        setNextFileName(nextFileName);
        setPosition(position);
        this.dataList = data;
    }

    public List<Object> getDataList() {
        return dataList;
    }

}