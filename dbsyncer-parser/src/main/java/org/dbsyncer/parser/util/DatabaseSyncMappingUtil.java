/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.util;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.sdk.model.DatabaseMapping;
import org.dbsyncer.sdk.model.TableMapping;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 整库迁移：库映射与 table_group 辅助方法。
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-07-20 15:30
 */
public abstract class DatabaseSyncMappingUtil {

    private DatabaseSyncMappingUtil() {
    }

    /**
     * 构建单表映射。
     */
    public static TableMapping toTableMapping(String sourceTable, String targetTable, int sortIndex) {
        TableMapping tm = new TableMapping();
        tm.setIndex(sortIndex);
        tm.setSourceTable(sourceTable);
        tm.setTargetTable(targetTable);
        return tm;
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
