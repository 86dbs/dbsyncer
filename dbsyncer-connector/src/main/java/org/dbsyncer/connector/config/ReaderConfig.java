package org.dbsyncer.connector.config;

import org.dbsyncer.connector.ConnectorMapper;

import java.util.List;
import java.util.Map;

public class ReaderConfig {

    private ConnectorMapper connectorMapper;
    private Map<String, String> command;
    private List<Object> args;
    private int pageIndex;
    private int pageSize;

    public ReaderConfig(ConnectorMapper connectorMapper, Map<String,String> command, List<Object> args, int pageIndex, int pageSize) {
        this.connectorMapper = connectorMapper;
        this.command = command;
        this.args = args;
        this.pageIndex = pageIndex;
        this.pageSize = pageSize;
    }

    public ConnectorMapper getConnectorMapper() {
        return connectorMapper;
    }

    public ReaderConfig setConnectorMapper(ConnectorMapper connectorMapper) {
        this.connectorMapper = connectorMapper;
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