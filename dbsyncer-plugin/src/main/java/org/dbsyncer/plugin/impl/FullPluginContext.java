package org.dbsyncer.plugin.impl;

import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.AbstractPluginContext;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.plugin.ReaderContext;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0s
 * @date 2022/6/30 16:04
 */
public final class FullPluginContext extends AbstractPluginContext implements ReaderContext {

    private Table sourceTable;

    private boolean supportedCursor;

    private List<Object> args;

    private Object[] cursors;

    private int pageIndex;

    private int pageSize;

    @Override
    public ModelEnum getModelEnum() {
        return ModelEnum.FULL;
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