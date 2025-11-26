/**
 * DBSyncer Copyright 2019-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.listener.event;

import org.dbsyncer.sdk.enums.ChangedEventTypeEnum;

import java.util.List;

/**
 * 监听行变更事件
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-06-15 20:00
 */
public final class RowChangedEvent extends CommonChangedEvent {
    private final List<Object> changedRow;
    private final List<String> columnNames;  // CDC 捕获的列名列表（按数据顺序）

    public RowChangedEvent(String sourceTableName, String event, List<Object> data) {
        this(sourceTableName, event, data, null, null, null);
    }

    public RowChangedEvent(String sourceTableName, String event, List<Object> data, String nextFileName, Object position) {
        this(sourceTableName, event, data, nextFileName, position, null);
    }

    public RowChangedEvent(String sourceTableName, String event, List<Object> data, String nextFileName, Object position, List<String> columnNames) {
        setSourceTableName(sourceTableName);
        setEvent(event);
        setNextFileName(nextFileName);
        setPosition(position);
        this.changedRow = data;
        this.columnNames = columnNames;
    }

    @Override
    public ChangedEventTypeEnum getType() {
        return ChangedEventTypeEnum.ROW;
    }

    @Override
    public List<Object> getChangedRow() {
        return changedRow;
    }

    /**
     * 获取 CDC 捕获的列名列表（按数据顺序）
     * 
     * @return 列名列表，如果为 null 表示使用 TableGroup 的字段信息
     */
    public List<String> getColumnNames() {
        return columnNames;
    }
}