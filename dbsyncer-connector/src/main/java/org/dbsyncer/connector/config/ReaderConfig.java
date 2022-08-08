package org.dbsyncer.connector.config;

import java.util.List;
import java.util.Map;

public class ReaderConfig {

    private Map<String, String> command;
    private List<Object> args;
    private String cursor;
    private int pageIndex;
    private int pageSize;

    public ReaderConfig(Map<String, String> command, List<Object> args, int pageIndex, int pageSize) {
        this.command = command;
        this.args = args;
        this.pageIndex = pageIndex;
        this.pageSize = pageSize;
    }

    public ReaderConfig(Map<String,String> command, List<Object> args, String cursor, int pageIndex, int pageSize) {
        this.command = command;
        this.args = args;
        this.cursor = cursor;
        this.pageIndex = pageIndex;
        this.pageSize = pageSize;
    }

    public Map<String, String> getCommand() {
        return command;
    }

    public List<Object> getArgs() {
        return args;
    }

    public String getCursor() {
        return cursor;
    }

    public void setCursor(String cursor) {
        this.cursor = cursor;
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public int getPageSize() {
        return pageSize;
    }
}