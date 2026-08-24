/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.plugin;

import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.model.Table;

import java.util.List;
import java.util.Map;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-02-08 02:17
 */
public abstract class AbstractBaseContext implements ReaderContext {

    /**
     * 数据源连接实例
     */
    private ConnectorInstance sourceConnectorInstance;

    /**
     * 执行命令
     */
    private Map<String, String> command;

    private Table sourceTable;

    private boolean supportedCursor;

    private List<Object> args;

    private Object[] cursors;

    private int pageIndex;

    private int pageSize;

    @Override
    public ConnectorInstance getSourceConnectorInstance() {
        return sourceConnectorInstance;
    }

    @Override
    public void setSourceConnectorInstance(ConnectorInstance sourceConnectorInstance) {
        this.sourceConnectorInstance = sourceConnectorInstance;
    }

    @Override
    public Map<String, String> getCommand() {
        return command;
    }

    @Override
    public void setCommand(Map<String, String> command) {
        this.command = command;
    }

    @Override
    public Table getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(Table sourceTable) {
        this.sourceTable = sourceTable;
    }

    @Override
    public boolean isSupportedCursor() {
        return supportedCursor;
    }

    public void setSupportedCursor(boolean supportedCursor) {
        this.supportedCursor = supportedCursor;
    }

    @Override
    public List<Object> getArgs() {
        return args;
    }

    public void setArgs(List<Object> args) {
        this.args = args;
    }

    @Override
    public Object[] getCursors() {
        return cursors;
    }

    public void setCursors(Object[] cursors) {
        this.cursors = cursors;
    }

    @Override
    public int getPageIndex() {
        return pageIndex;
    }

    public void setPageIndex(int pageIndex) {
        this.pageIndex = pageIndex;
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }
}
