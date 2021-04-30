package org.dbsyncer.connector.config;

import java.util.List;
import java.util.Map;

public class ReaderConfig {

    private ConnectorConfig config;
    private Map<String, String> command;
    private List<Object> args;
    private int pageIndex;
    private int pageSize;

    public ReaderConfig(ConnectorConfig config, Map<String,String> command, List<Object> args, int pageIndex, int pageSize) {
        this.config = config;
        this.command = command;
        this.args = args;
        this.pageIndex = pageIndex;
        this.pageSize = pageSize;
    }

    public ConnectorConfig getConfig() {
        return config;
    }

    public ReaderConfig setConfig(ConnectorConfig config) {
        this.config = config;
        return this;
    }

    public Map<String, String> getCommand() {
        return command;
    }

    public ReaderConfig setCommand(Map<String, String> command) {
        this.command = command;
        return this;
    }

    public List<Object> getArgs() {
        return args;
    }

    public ReaderConfig setArgs(List<Object> args) {
        this.args = args;
        return this;
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public ReaderConfig setPageIndex(int pageIndex) {
        this.pageIndex = pageIndex;
        return this;
    }

    public int getPageSize() {
        return pageSize;
    }

    public ReaderConfig setPageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }
}