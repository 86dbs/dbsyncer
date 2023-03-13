package org.dbsyncer.listener.quartz;

import java.util.List;
import java.util.Map;

public class TableGroupCommand {

    private List<String> primaryKeys;

    private Map<String, String> command;

    public TableGroupCommand(List<String> primaryKeys, Map<String, String> command) {
        this.primaryKeys = primaryKeys;
        this.command = command;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public Map<String, String> getCommand() {
        return command;
    }
}