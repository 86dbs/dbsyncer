package org.dbsyncer.listener.quartz;

import java.util.Map;

public class TableGroupCommand {

    private String pk;

    private Map<String, String> command;

    public TableGroupCommand(String pk, Map<String, String> command) {
        this.pk = pk;
        this.command = command;
    }

    public String getPk() {
        return pk;
    }

    public Map<String, String> getCommand() {
        return command;
    }
}