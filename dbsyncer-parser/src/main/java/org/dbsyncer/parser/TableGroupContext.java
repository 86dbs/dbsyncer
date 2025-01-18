/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.TableGroupPicker;

import java.util.List;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-01-16 23:48
 */
public interface TableGroupContext {

    void put(Mapping mapping, List<TableGroup> tableGroups);

    List<TableGroupPicker> getTableGroupPickers(String metaId, String tableName);

    void clear(String metaId);
}
