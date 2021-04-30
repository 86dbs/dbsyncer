/**
 * DBSyncer Copyright 2019-2024 All Rights Reserved.
 */
package org.dbsyncer.common.event;

import org.dbsyncer.common.util.JsonUtil;

import java.util.List;
import java.util.Map;

/**
 * 监听行变更事件
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-06-15 20:00
 */
public class RowChangedEvent {

    private int                 tableGroupIndex;
    private String              tableName;
    private String              event;
    private List<Object>        beforeData;
    private List<Object>        afterData;
    private Map<String, Object> before;
    private Map<String, Object> after;

    public RowChangedEvent(int tableGroupIndex, String event, Map<String, Object> before, Map<String, Object> after) {
        this.tableGroupIndex = tableGroupIndex;
        this.event = event;
        this.before = before;
        this.after = after;
    }

    public RowChangedEvent(String tableName, String event, List<Object> beforeData, List<Object> afterData) {
        this.tableName = tableName;
        this.event = event;
        this.beforeData = beforeData;
        this.afterData = afterData;
    }

    public int getTableGroupIndex() {
        return tableGroupIndex;
    }

    public String getTableName() {
        return tableName;
    }

    public String getEvent() {
        return event;
    }

    public List<Object> getBeforeData() {
        return beforeData;
    }

    public List<Object> getAfterData() {
        return afterData;
    }

    public Map<String, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }

    public Map<String, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }

    @Override
    public String toString() {
        return JsonUtil.objToJson(this);
    }
}