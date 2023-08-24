/**
 * DBSyncer Copyright 2019-2024 All Rights Reserved.
 */
package org.dbsyncer.common.event;

import java.util.Map;

/**
 * 分页变更事件
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2023-08-20 20:00
 */
public final class PageChangedEvent extends CommonChangedEvent {

    private int tableGroupIndex;

    public PageChangedEvent(int index, String event, Map<String, Object> changedRow) {
        this.tableGroupIndex = index;
        setEvent(event);
        setChangedRow(changedRow);
    }

    public int getTableGroupIndex() {
        return tableGroupIndex;
    }
}