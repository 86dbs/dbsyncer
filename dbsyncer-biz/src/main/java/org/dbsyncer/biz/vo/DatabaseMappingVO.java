/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import org.dbsyncer.sdk.model.DatabaseMapping;
import org.dbsyncer.sdk.model.TableMapping;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 库映射业务视图：库维字段继承 {@link DatabaseMapping}，表映射仅用于接口入参/回显。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-23 10:45
 */
public final class DatabaseMappingVO extends DatabaseMapping {

    /**
     * 表映射（源表 -&gt; 目标表），对应 dbsyncer_table_group，不写入 task.JSON
     */
    private List<TableMapping> tableMappings = new ArrayList<>();

    public List<TableMapping> getTableMappings() {
        return tableMappings;
    }

    public void setTableMappings(List<TableMapping> tableMappings) {
        this.tableMappings = tableMappings == null ? new ArrayList<>() : tableMappings;
    }

    /**
     * 按 index 升序返回表映射列表（副本）。
     */
    public List<TableMapping> getSortedTableMappings() {
        if (tableMappings == null || tableMappings.isEmpty()) {
            return Collections.emptyList();
        }
        return tableMappings.stream()
                .sorted(Comparator.comparingInt(TableMapping::getIndex))
                .collect(Collectors.toList());
    }

    /**
     * 从库映射领域模型复制库维字段。
     */
    public static DatabaseMappingVO from(DatabaseMapping mapping) {
        DatabaseMappingVO vo = new DatabaseMappingVO();
        if (mapping != null) {
            BeanUtils.copyProperties(mapping, vo);
        }
        return vo;
    }

    /**
     * 转为可持久化的库映射（不含表）。
     */
    public DatabaseMapping toDatabaseMapping() {
        DatabaseMapping mapping = new DatabaseMapping();
        BeanUtils.copyProperties(this, mapping);
        return mapping;
    }
}
