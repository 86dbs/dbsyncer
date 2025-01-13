package org.dbsyncer.sdk.model;

import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TableGroupQuartzCommand {

    private final Table table;

    private final List<Field> fields;

    private final Map<String, String> command;

    private final List<String> primaryKeys;

    public TableGroupQuartzCommand(Table table, List<Field> fields, Map<String, String> command) {
        this.table = table;
        this.fields = fields;
        this.command = command;
        this.primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(table);;
    }

    public Table getTable() {
        return table;
    }

    public List<Field> getFields() {
        return fields;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public Map<String, String> getCommand() {
        return command;
    }

    public List<Object> getChangedRow(Map<String, Object> row) {
        List<Object> changedRow = new ArrayList<>(fields.size());
        fields.forEach(field -> changedRow.add(row.get(field.getName())));
        return changedRow;
    }
}