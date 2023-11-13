/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.consumer.impl;

import org.dbsyncer.common.event.ScanChangedEvent;
import org.dbsyncer.parser.consumer.AbstractConsumer;
import org.dbsyncer.parser.model.FieldPicker;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.PickerUtil;

import java.util.LinkedList;
import java.util.List;

/**
 * 定时消费
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-12 02:18
 */
public final class QuartzConsumer extends AbstractConsumer<ScanChangedEvent> {
    private List<FieldPicker> tablePicker = new LinkedList<>();

    @Override
    public void postProcessBeforeInitialization() {
        tableGroups.forEach(t -> {
            tablePicker.add(new FieldPicker(PickerUtil.mergeTableGroupConfig(mapping, t)));
            bind(t.getId());
        });
    }

    @Override
    public void onChange(ScanChangedEvent event) {
        final FieldPicker picker = tablePicker.get(event.getTableGroupIndex());
        TableGroup tableGroup = picker.getTableGroup();
        event.setSourceTableName(tableGroup.getSourceTable().getName());

        // 定时暂不支持触发刷新增量点事件
        execute(tableGroup.getId(), event);
    }
}