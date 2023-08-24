/**
 * DBSyncer Copyright 2019-2024 All Rights Reserved.
 */
package org.dbsyncer.common.event;

import java.util.Map;

/**
 * 通用变更事件
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2023-08-20 20:00
 */
public class CommonChangedEvent implements ChangedEvent {

    /**
     * 变更表名称
     */
    private String sourceTableName;
    /**
     * 变更事件
     */
    private String event;
    /**
     * 变更行数据
     */
    private Map<String, Object> changedRow;
    /**
     * 增量偏移量
     */
    private ChangedOffset changedOffset = new ChangedOffset();

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public Map<String, Object> getChangedRow() {
        return changedRow;
    }

    public void setChangedRow(Map<String, Object> changedRow) {
        this.changedRow = changedRow;
    }

    @Override
    public ChangedOffset getChangedOffset() {
        return changedOffset;
    }

    public void setNextFileName(String nextFileName) {
        changedOffset.setNextFileName(nextFileName);
    }

    public void setPosition(Object position) {
        changedOffset.setPosition(position);
    }
}