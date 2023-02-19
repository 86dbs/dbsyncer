package org.dbsyncer.listener.quartz;

import java.util.Map;
import java.util.Set;

public class TableGroupCommand {

    private Set<String> primaryKeys;

    private Map<String, String> command;

    public TableGroupCommand(Set<String> primaryKeys, Map<String, String> command) {
        this.primaryKeys = primaryKeys;
        this.command = command;
    }

    public Set<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public Map<String, String> getCommand() {
        return command;
    }
}