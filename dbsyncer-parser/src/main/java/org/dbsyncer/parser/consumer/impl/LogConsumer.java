/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.consumer.impl;

import org.dbsyncer.listener.event.CommonChangedEvent;
import org.dbsyncer.listener.event.DDLChangedEvent;
import org.dbsyncer.listener.event.RowChangedEvent;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.parser.consumer.AbstractConsumer;
import org.dbsyncer.parser.model.FieldPicker;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.PickerUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 日志消费
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-12 02:25
 */
public final class LogConsumer extends AbstractConsumer<RowChangedEvent> {
    private Map<String, List<FieldPicker>> tablePicker = new LinkedHashMap<>();

    //判断上次是否为ddl，是ddl需要强制刷新下picker
    private boolean ddlChanged;

    @Override
    public void postProcessBeforeInitialization() {
        addTablePicker(true);
    }

    @Override
    public void onChange(RowChangedEvent event) {
        // 需要强制刷新 fix https://gitee.com/ghi/dbsyncer/issues/I8DJUR
        if (ddlChanged) {
            addTablePicker(false);
            ddlChanged = false;
        }
        process(event, picker -> {
            final Map<String, Object> changedRow = picker.getColumns(event.getDataList());
            if (picker.filter(changedRow)) {
                event.setChangedRow(changedRow);
                execute(picker.getTableGroup().getId(), event);
            }
        });
    }

    @Override
    public void onDDLChanged(DDLChangedEvent event) {
        ddlChanged = true;
        process(event, picker -> execute(picker.getTableGroup().getId(), event));
    }

    private void process(CommonChangedEvent event, Consumer<FieldPicker> consumer) {
        // 处理过程有异常向上抛
        List<FieldPicker> pickers = tablePicker.get(event.getSourceTableName());
        if (!CollectionUtils.isEmpty(pickers)) {
            // 触发刷新增量点事件
            event.getChangedOffset().setRefreshOffset(true);
            pickers.forEach(picker -> consumer.accept(picker));
        }
    }

    private void addTablePicker(boolean bindBufferActuatorRouter) {
        this.tablePicker.clear();
        this.tableGroups.forEach(t -> {
            final Table table = t.getSourceTable();
            final String tableName = table.getName();
            tablePicker.putIfAbsent(tableName, new ArrayList<>());
            TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, t);
            tablePicker.get(tableName).add(new FieldPicker(group, group.getFilter(), table.getColumn(), group.getFieldMapping()));
            // 是否注册到路由服务中
            if (bindBufferActuatorRouter) {
                bind(group.getId());
            }
        });
    }

}