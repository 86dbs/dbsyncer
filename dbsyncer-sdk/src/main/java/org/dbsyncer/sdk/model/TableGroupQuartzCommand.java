package org.dbsyncer.sdk.model;

import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.util.List;
import java.util.Map;

public class TableGroupQuartzCommand {

    private Table table;

    private List<String> primaryKeys;

    private Map<String, String> command;

    public TableGroupQuartzCommand(Table table, Map<String, String> command) {
        this.table = table;
        this.command = command;
        this.primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(table);;
    }

    public Table getTable() {
        return table;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public Map<String, String> getCommand() {
        return command;
    }
}