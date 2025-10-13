package org.dbsyncer.sdk.model;

import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableGroupQuartzCommand {

    private final Table table;

    private final Table targetTable;

    private final List<Field> fields;

    private final Map<String, String> command;

    private final Plugin plugin;

    private final String pluginExtInfo;

    private final List<String> primaryKeys;

    public TableGroupQuartzCommand(Table table, List<Field> fields, Table targetTable, Map<String, String> command, Plugin plugin, String pluginExtInfo) {
        this.table = table;
        this.fields = fields;
        this.targetTable = targetTable;
        this.command = command;
        this.plugin = plugin;
        this.pluginExtInfo = pluginExtInfo;
        this.primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(table);
    }

    public Table getTable() {
        return table;
    }

    public Table getTargetTable() {
        return targetTable;
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

    public Plugin getPlugin() {
        return plugin;
    }

    public String getPluginExtInfo() {
        return pluginExtInfo;
    }

    public List<Object> getChangedRow(Map<String, Object> row) {
        return fields.stream().map(field -> row.get(field.getName())).collect(Collectors.toList());
    }
}