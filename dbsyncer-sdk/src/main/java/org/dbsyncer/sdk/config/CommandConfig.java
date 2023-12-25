package org.dbsyncer.sdk.config;

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

    private ConnectorConfig connectorConfig;

    public CommandConfig(String connectorType, Table table, ConnectorConfig connectorConfig, List<Filter> filter) {
        this.connectorType = connectorType;
        this.table = table;
        this.filter = filter;
        this.connectorConfig = connectorConfig;
    }

    public String getConnectorType() {
        return connectorType;
    }

    public Table getTable() {
        return table;
    }

    public List<Filter> getFilter() {
        return filter;
    }

    public ConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }
}