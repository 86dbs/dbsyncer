package org.dbsyncer.connector.config;

import org.dbsyncer.common.model.AbstractConnectorConfig;
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

    private AbstractConnectorConfig connectorConfig;

    public CommandConfig(String type, Table table, Table originalTable, AbstractConnectorConfig connectorConfig) {
        this(type, table, originalTable, connectorConfig, null);
    }

    public CommandConfig(String type, Table table, Table originalTable, AbstractConnectorConfig connectorConfig, List<Filter> filter) {
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

    public AbstractConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }
}