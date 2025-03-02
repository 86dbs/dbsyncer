/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.parser.TableGroupContext;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.TableGroupPicker;
import org.dbsyncer.parser.util.PickerUtil;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-01-16 23:34
 */
@Component
public final class TableGroupContextImpl implements TableGroupContext {

    /**
     * 驱动表映射关系
     */
    private final Map<String, InnerMapping> tableGroupMap = new ConcurrentHashMap<>();

    @Override
    public void put(Mapping mapping, List<TableGroup> tableGroups) {
        tableGroupMap.computeIfAbsent(mapping.getMetaId(), k -> {
            InnerMapping innerMap = new InnerMapping();
            tableGroups.forEach(tableGroup -> {
                String sourceTableName = tableGroup.getSourceTable().getName();
                innerMap.add(sourceTableName, PickerUtil.mergeTableGroupConfig(mapping, tableGroup));
            });
            return innerMap;
        });
    }

    @Override
    public void update(Mapping mapping, List<TableGroup> tableGroups) {
        tableGroupMap.computeIfPresent(mapping.getMetaId(), (k, innerMap) -> {
            // 先清空表映射关系，再更新表映射关系
            tableGroups.stream().findFirst().ifPresent(tableGroup -> innerMap.remove(tableGroup.getSourceTable().getName()));
            tableGroups.forEach(tableGroup -> {
                String sourceTableName = tableGroup.getSourceTable().getName();
                innerMap.add(sourceTableName, PickerUtil.mergeTableGroupConfig(mapping, tableGroup));
            });
            return innerMap;
        });
    }

    @Override
    public List<TableGroupPicker> getTableGroupPickers(String metaId, String tableName) {
        List<TableGroupPicker> list = new ArrayList<>();
        tableGroupMap.computeIfPresent(metaId, (k, innerMapping) -> {
            innerMapping.pickerMap.computeIfPresent(tableName, (x, pickers) -> {
                list.addAll(pickers);
                return pickers;
            });
            return innerMapping;
        });
        return list;
    }

    @Override
    public void clear(String metaId) {
        tableGroupMap.remove(metaId);
    }

    static final class InnerMapping {
        Map<String, List<TableGroupPicker>> pickerMap = new ConcurrentHashMap<>();

        public void add(String tableName, TableGroup tableGroup) {
            pickerMap.computeIfAbsent(tableName, k -> new ArrayList<>()).add(new TableGroupPicker(tableGroup));
        }

        public void remove(String tableName) {
            pickerMap.remove(tableName);
        }
    }
}
