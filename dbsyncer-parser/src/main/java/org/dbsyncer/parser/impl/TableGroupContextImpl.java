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

    private final List<TableGroupPicker> EMPTY_LIST = new ArrayList<>();

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
    public List<TableGroupPicker> getTableGroupPickers(String metaId, String tableName) {
        InnerMapping innerMapping = tableGroupMap.get(metaId);
        if (innerMapping != null) {
            return innerMapping.get(tableName);
        }
        return EMPTY_LIST;
    }

    @Override
    public void clear(String metaId) {
        tableGroupMap.remove(metaId);
    }

    static final class InnerMapping {
        Map<String, List<TableGroupPicker>> pickerMap = new ConcurrentHashMap<>();

        public List<TableGroupPicker> get(String tableName) {
            return pickerMap.get(tableName);
        }

        public void add(String tableName, TableGroup tableGroup) {
            pickerMap.computeIfAbsent(tableName, k -> new ArrayList<>()).add(new TableGroupPicker(tableGroup));
        }
    }
}
