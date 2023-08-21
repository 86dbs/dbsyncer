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

    private int tableGroupIndex;
    private String sourceTableName;
    private String event;
    private List<Object> dataList;
    private Map<String, Object> dataMap;
    private String nextFileName;
    private Object position;

    public RowChangedEvent(int tableGroupIndex, String event, Map<String, Object> data) {
        this.tableGroupIndex = tableGroupIndex;
        this.event = event;
        this.dataMap = data;
    }

    public RowChangedEvent(String sourceTableName, String event, List<Object> data) {
        this.sourceTableName = sourceTableName;
        this.event = event;
        this.dataList = data;
    }

    public int getTableGroupIndex() {
        return tableGroupIndex;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getEvent() {
        return event;
    }

    public List<Object> getDataList() {
        return dataList;
    }

    public void setDataList(List<Object> dataList) {
        this.dataList = dataList;
    }

    public Map<String, Object> getDataMap() {
        return dataMap;
    }

    public void setDataMap(Map<String, Object> dataMap) {
        this.dataMap = dataMap;
    }

    public String getNextFileName() {
        return nextFileName;
    }

    public RowChangedEvent setNextFileName(String nextFileName) {
        this.nextFileName = nextFileName;
        return this;
    }

    public Object getPosition() {
        return position;
    }

    public RowChangedEvent setPosition(Object position) {
        this.position = position;
        return this;
    }

    @Override
    public String toString() {
        return JsonUtil.objToJson(this);
    }
}