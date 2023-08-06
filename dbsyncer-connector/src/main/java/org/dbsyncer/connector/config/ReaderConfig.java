package org.dbsyncer.connector.config;

import org.dbsyncer.connector.model.Table;

import java.util.List;
import java.util.Map;

public class ReaderConfig {

    private Table table;
    private Map<String, String> command;
    private List<Object> args;
    private boolean supportedCursor;
    private Object[] cursors;
    private int pageIndex;
    private int pageSize;

    public ReaderConfig(Table table, Map<String,String> command, List<Object> args, boolean supportedCursor, Object[] cursors, int pageIndex, int pageSize) {
        this.table = table;
        this.command = command;
        this.args = args;
        this.supportedCursor = supportedCursor;
        this.cursors = cursors;
        this.pageIndex = pageIndex;
        this.pageSize = pageSize;
    }

    public Table getTable() {
        return table;
    }

    public Map<String, String> getCommand() {
        return command;
    }

    public List<Object> getArgs() {
        return args;
    }

    public Object[] getCursors() {
        return cursors;
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public int getPageSize() {
        return pageSize;
    }

    public boolean isSupportedCursor() {
        return supportedCursor;
    }
}