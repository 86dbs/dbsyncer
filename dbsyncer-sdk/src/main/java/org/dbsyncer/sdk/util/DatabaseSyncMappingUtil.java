/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.util;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.DatabaseMapping;
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
     * @param sourceTable       源表
     * @param targetTable       目标表
     * @param sortIndex         全局排序号
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
     * 按库维度聚合 table_group 行，还原 DatabaseMapping 列表。
     *
     * @param rows 每行含 connector/database/schema/table/sortIndex 键(与 ConfigConstant.TABLE_GROUP_* 一致)
     * @return 库映射列表
     */
    public static List<DatabaseMapping> rebuildDatabaseMappings(List<TableGroupRow> rows) {
        if (CollectionUtils.isEmpty(rows)) {
            return new ArrayList<>();
        }
        Map<String, DatabaseMapping> mappingMap = new LinkedHashMap<>();
        rows.stream().sorted(Comparator.comparingInt(TableGroupRow::getSortIndex)).forEach(row -> {
            String key = String.join("|",
                    StringUtil.getIfBlank(row.getSourceConnectorId(), ""),
                    StringUtil.getIfBlank(row.getTargetConnectorId(), ""),
                    StringUtil.getIfBlank(row.getSourceDatabase(), ""),
                    StringUtil.getIfBlank(row.getTargetDatabase(), ""),
                    StringUtil.getIfBlank(row.getSourceSchema(), ""),
                    StringUtil.getIfBlank(row.getTargetSchema(), ""));
            DatabaseMapping mapping = mappingMap.computeIfAbsent(key, k -> {
                DatabaseMapping dm = new DatabaseMapping();
                dm.setSourceConnectorId(row.getSourceConnectorId());
                dm.setTargetConnectorId(row.getTargetConnectorId());
                dm.setSourceDatabase(row.getSourceDatabase());
                dm.setTargetDatabase(row.getTargetDatabase());
                dm.setSourceSchema(row.getSourceSchema());
                dm.setTargetSchema(row.getTargetSchema());
                dm.setTableMappings(new ArrayList<>());
                return dm;
            });
            if (StringUtil.isNotBlank(row.getSourceTable()) && StringUtil.isNotBlank(row.getTargetTable())) {
                mapping.getTableMappings().add(toTableMapping(row.getSourceTable(), row.getTargetTable(), row.getSortIndex()));
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

    /**
     * table_group 关联行(轻量 DTO，避免 parser 模块依赖)。
     */
    public static final class TableGroupRow {
        private int sortIndex;
        private String sourceConnectorId;
        private String targetConnectorId;
        private String sourceDatabase;
        private String targetDatabase;
        private String sourceSchema;
        private String targetSchema;
        private String sourceTable;
        private String targetTable;

        public int getSortIndex() {
            return sortIndex;
        }

        public TableGroupRow setSortIndex(int sortIndex) {
            this.sortIndex = sortIndex;
            return this;
        }

        public String getSourceConnectorId() {
            return sourceConnectorId;
        }

        public TableGroupRow setSourceConnectorId(String sourceConnectorId) {
            this.sourceConnectorId = sourceConnectorId;
            return this;
        }

        public String getTargetConnectorId() {
            return targetConnectorId;
        }

        public TableGroupRow setTargetConnectorId(String targetConnectorId) {
            this.targetConnectorId = targetConnectorId;
            return this;
        }

        public String getSourceDatabase() {
            return sourceDatabase;
        }

        public TableGroupRow setSourceDatabase(String sourceDatabase) {
            this.sourceDatabase = sourceDatabase;
            return this;
        }

        public String getTargetDatabase() {
            return targetDatabase;
        }

        public TableGroupRow setTargetDatabase(String targetDatabase) {
            this.targetDatabase = targetDatabase;
            return this;
        }

        public String getSourceSchema() {
            return sourceSchema;
        }

        public TableGroupRow setSourceSchema(String sourceSchema) {
            this.sourceSchema = sourceSchema;
            return this;
        }

        public String getTargetSchema() {
            return targetSchema;
        }

        public TableGroupRow setTargetSchema(String targetSchema) {
            this.targetSchema = targetSchema;
            return this;
        }

        public String getSourceTable() {
            return sourceTable;
        }

        public TableGroupRow setSourceTable(String sourceTable) {
            this.sourceTable = sourceTable;
            return this;
        }

        public String getTargetTable() {
            return targetTable;
        }

        public TableGroupRow setTargetTable(String targetTable) {
            this.targetTable = targetTable;
            return this;
        }
    }
}
