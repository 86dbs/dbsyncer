/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector;

import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.MetaContext;

import java.util.Map;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-12 00:46
 */
public class DefaultMetaContext implements MetaContext {
    private Table sourceTable;
    private Map<String, String> command;
    private ConnectorInstance sourceConnectorInstance;

    @Override
    public Table getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(Table sourceTable) {
        this.sourceTable = sourceTable;
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
    public ConnectorInstance getSourceConnectorInstance() {
        return sourceConnectorInstance;
    }

    @Override
    public void setSourceConnectorInstance(ConnectorInstance sourceConnectorInstance) {
        this.sourceConnectorInstance = sourceConnectorInstance;
    }
}
