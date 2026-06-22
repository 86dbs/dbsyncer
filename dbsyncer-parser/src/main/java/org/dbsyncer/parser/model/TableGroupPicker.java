/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.sdk.model.Field;

import java.util.List;
import java.util.Map;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2025-01-18 22:53
 */
public class TableGroupPicker {

    private final TableGroup tableGroup;

    private final Picker picker;

    private final List<Field> sourceFields;
    private final List<Field> targetFields;
    private final Map<String, Field> targetFieldMap;

    public TableGroupPicker(TableGroup tableGroup) {
        this.tableGroup = tableGroup;
        this.picker = new Picker(tableGroup);
        this.sourceFields = picker.getSourceFields();
        this.targetFields = picker.getTargetFields();
        this.targetFieldMap = picker.getTargetFieldMap();
    }

    public TableGroup getTableGroup() {
        return tableGroup;
    }

    public Picker getPicker() {
        return picker;
    }

    public List<Field> getSourceFields() {
        return sourceFields;
    }

    public List<Field> getTargetFields() {
        return targetFields;
    }

    public Map<String, Field> getTargetFieldMap() {
        return targetFieldMap;
    }
}
