/**
 * DBSyncer Copyright 2019-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.listener.event;

import org.dbsyncer.sdk.enums.ChangedEventTypeEnum;

import java.util.List;

/**
 * 定时扫表变更事件
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2023-08-20 20:00
 */
public final class ScanChangedEvent extends CommonChangedEvent {

    private final List<Object> changedRow;

    public ScanChangedEvent(String sourceTableName, String event, List<Object> data) {
        setSourceTableName(sourceTableName);
        setEvent(event);
        this.changedRow = data;
    }

    @Override
    public ChangedEventTypeEnum getType() {
        return ChangedEventTypeEnum.SCAN;
    }

    @Override
    public List<Object> getChangedRow() {
        return changedRow;
    }
}
