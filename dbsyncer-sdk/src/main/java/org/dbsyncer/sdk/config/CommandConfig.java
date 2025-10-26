/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.config;

import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.Filter;
import org.dbsyncer.sdk.model.Table;

import java.util.List;

/**
 * 查询同步参数模板
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
public class CommandConfig {

    private String connectorType;

    private Table table;

    private List<Filter> filter;

    private String schema;

    private ConnectorInstance connectorInstance;

    public CommandConfig(String connectorType, String schema, Table table, ConnectorInstance connectorInstance, List<Filter> filter) {
        this.connectorType = connectorType;
        this.schema = schema;
        this.table = table;
        this.connectorInstance = connectorInstance;
        this.filter = filter;
    }

    public String getConnectorType() {
        return connectorType;
    }

    public String getSchema() {
        return schema;
    }

    public Table getTable() {
        return table;
    }

    public List<Filter> getFilter() {
        return filter;
    }

    public ConnectorConfig getConnectorConfig() {
        return connectorInstance.getConfig();
    }

    public ConnectorInstance getConnectorInstance() {
        return connectorInstance;
    }
}