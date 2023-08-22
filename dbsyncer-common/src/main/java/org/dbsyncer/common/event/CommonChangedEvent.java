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
     * 增量文件名称
     */
    private String nextFileName;
    /**
     * 增量偏移量
     */
    private Object position;

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

    public String getNextFileName() {
        return nextFileName;
    }

    public void setNextFileName(String nextFileName) {
        this.nextFileName = nextFileName;
    }

    public Object getPosition() {
        return position;
    }

    public CommonChangedEvent setPosition(Object position) {
        this.position = position;
        return this;
    }
}