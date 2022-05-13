package org.dbsyncer.connector.config;

import org.dbsyncer.connector.model.Filter;
import org.dbsyncer.connector.model.Table;

import java.util.List;

/**
 * 查询同步参数模板
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
public class CommandConfig {

    private String type;

    private Table table;

    private Table originalTable;

    private List<Filter> filter;

    private ConnectorConfig connectorConfig;

    public CommandConfig(String type, Table table, Table originalTable) {
        this.type = type;
        this.table = table;
        this.originalTable = originalTable;
    }

    public CommandConfig(String type, Table table, Table originalTable, List<Filter> filter, ConnectorConfig connectorConfig) {
        this.type = type;
        this.table = table;
        this.originalTable = originalTable;
        this.filter = filter;
        this.connectorConfig = connectorConfig;
    }

    public String getType() {
        return type;
    }

    public Table getTable() {
        return table;
    }

    public List<Filter> getFilter() {
        return filter;
    }

    public Table getOriginalTable() {
        return originalTable;
    }

    public ConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }
}