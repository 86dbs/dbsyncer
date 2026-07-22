/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.util;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.model.DatabaseMapping;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.TableMapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 整库迁移：table_group 与 DatabaseMapping 视图互转。
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-07-20 15:30
 */
public abstract class DatabaseSyncMappingUtil {

    private DatabaseSyncMappingUtil() {
    }

    /**
     * 从 table_group 拆分列还原库映射视图(运行时/前端展示)。
     *
     * @param sourceTable 源表
     * @param targetTable 目标表
     * @param sortIndex   全局排序号
     * @return 单条 table_group 对应的表映射
     */
    public static TableMapping toTableMapping(String sourceTable, String targetTable, int sortIndex) {
        TableMapping tm = new TableMapping();
        tm.setIndex(sortIndex);
        tm.setSourceTable(sourceTable);
        tm.setTargetTable(targetTable);
        return tm;
    }

    /**
     * 按库维度聚合 table_group，还原 DatabaseMapping 列表。
     *
     * @param tableGroups 表映射列表
     * @return 库映射列表
     */
    public static List<DatabaseMapping> rebuildDatabaseMappings(List<TableGroup> tableGroups) {
        if (CollectionUtils.isEmpty(tableGroups)) {
            return new ArrayList<>();
        }
        Map<String, DatabaseMapping> mappingMap = new LinkedHashMap<>();
        tableGroups.stream().sorted(Comparator.comparingInt(TableGroup::getIndex)).forEach(group -> {
            String key = group.buildDatabaseMappingKey();
            DatabaseMapping mapping = mappingMap.computeIfAbsent(key, k -> {
                DatabaseMapping dm = new DatabaseMapping();
                dm.setSourceConnectorId(group.getSourceConnectorId());
                dm.setTargetConnectorId(group.getTargetConnectorId());
                dm.setSourceDatabase(group.getSourceDatabase());
                dm.setTargetDatabase(group.getTargetDatabase());
                dm.setSourceSchema(group.getSourceSchema());
                dm.setTargetSchema(group.getTargetSchema());
                dm.setTableMappings(new ArrayList<>());
                return dm;
            });
            Table sourceTable = group.getSourceTable();
            Table targetTable = group.getTargetTable();
            if (sourceTable != null && targetTable != null
                    && StringUtil.isNotBlank(sourceTable.getName())
                    && StringUtil.isNotBlank(targetTable.getName())) {
                mapping.getTableMappings().add(toTableMapping(sourceTable.getName(), targetTable.getName(), group.getIndex()));
            }
        });
        List<DatabaseMapping> result = new ArrayList<>(mappingMap.values());
        for (int i = 0; i < result.size(); i++) {
            result.get(i).setIndex(i + 1);
        }
        return result;
    }

    /**
     * 按 index 升序返回库映射列表（副本）。
     */
    public static List<DatabaseMapping> sortByIndex(List<DatabaseMapping> mappings) {
        if (CollectionUtils.isEmpty(mappings)) {
            return Collections.emptyList();
        }
        return mappings.stream()
                .sorted(Comparator.comparingInt(DatabaseMapping::getIndex))
                .collect(Collectors.toList());
    }
}
