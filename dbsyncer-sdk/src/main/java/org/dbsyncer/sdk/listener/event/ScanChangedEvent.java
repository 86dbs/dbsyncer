/**
 * DBSyncer Copyright 2019-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.listener.event;

import java.util.Map;

/**
 * 定时扫表变更事件
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2023-08-20 20:00
 */
public final class ScanChangedEvent extends CommonChangedEvent {

    private int tableGroupIndex;

    public ScanChangedEvent(int index, String event, Map<String, Object> changedRow) {
        this.tableGroupIndex = index;
        setEvent(event);
        setChangedRow(changedRow);
    }

    public int getTableGroupIndex() {
        return tableGroupIndex;
    }
}