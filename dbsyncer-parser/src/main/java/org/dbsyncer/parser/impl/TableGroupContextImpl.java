/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TableGroupContext;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.TableGroupPicker;
import org.dbsyncer.parser.util.PickerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
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

    private static final Logger logger = LoggerFactory.getLogger(TableGroupContextImpl.class);

    /**
     * 驱动表映射关系
     */
    private final Map<String, InnerMapping> tableGroupMap = new ConcurrentHashMap<>();

    @Resource
    private ProfileComponent profileComponent;

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
            // 强制刷新缓存：先清空表映射关系，再更新表映射关系
            // 这确保了 DDL 事件处理完成后，后续的 DML 事件使用最新的 fieldMapping
            tableGroups.forEach(tableGroup -> {
                String sourceTableName = tableGroup.getSourceTable().getName();
                // 先移除旧的 TableGroupPicker（如果存在）
                innerMap.remove(sourceTableName);
                // 再添加新的 TableGroupPicker（使用更新后的 fieldMapping）
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

        // 如果缓存未命中，尝试从持久化存储加载（Read-Through 模式）
        if (CollectionUtils.isEmpty(list)) {
            // 从 Meta 获取 mappingId
            Meta meta = profileComponent.getMeta(metaId);
            assert meta != null;
            // 获取 Mapping 和 TableGroup 列表
            Mapping mapping = profileComponent.getMapping(meta.getMappingId());
            assert mapping != null;
            List<TableGroup> tableGroups = null;
            try {
                tableGroups = profileComponent.getSortedTableGroupAll(meta.getMappingId());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (!CollectionUtils.isEmpty(tableGroups)) {
                // 先清理可能存在的空缓存（因为 put() 使用 computeIfAbsent，如果 metaId 已存在不会更新）
                clear(metaId);
                // 填充缓存
                put(mapping, tableGroups);
                // 从缓存中重新获取（避免递归调用）
                tableGroupMap.computeIfPresent(metaId, (k, innerMapping) -> {
                    innerMapping.pickerMap.computeIfPresent(tableName, (x, pickers) -> {
                        list.addAll(pickers);
                        return pickers;
                    });
                    return innerMapping;
                });
            }
        }

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
