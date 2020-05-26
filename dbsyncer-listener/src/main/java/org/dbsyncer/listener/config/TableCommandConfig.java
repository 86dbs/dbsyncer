package org.dbsyncer.listener.config;

import java.util.Map;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-25 23:52
 */
public class TableCommandConfig {

    private String table;

    private Map<String, String> command;

    public TableCommandConfig(String table, Map<String, String> command) {
        this.table = table;
        this.command = command;
    }

    public String getTable() {
        return table;
    }

    public Map<String, String> getCommand() {
        return command;
    }

}