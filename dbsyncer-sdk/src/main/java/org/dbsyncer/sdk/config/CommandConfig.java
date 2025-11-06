/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
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

    private ConnectorInstance connectorInstance;

    // 缓存的字段列表SQL片段（避免重复构建）
    private String cachedFieldListSql;

    // 缓存的主键列表字符串（避免重复构建）
    private String cachedPrimaryKeys;

    public CommandConfig(String connectorType, Table table, ConnectorInstance connectorInstance, List<Filter> filter) {
        this.connectorType = connectorType;
        this.table = table;
        this.filter = filter;
        this.connectorInstance = connectorInstance;
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
        return connectorInstance.getConfig();
    }

    /**
     * 获取连接器实例
     * 注意：此方法不应被序列化，因为 ConnectorInstance 包含 Connection 对象，不应被序列化
     *
     * @return
     */
    @JsonIgnore
    public ConnectorInstance getConnectorInstance() {
        return connectorInstance;
    }

    public String getCachedFieldListSql() {
        return cachedFieldListSql;
    }

    public void setCachedFieldListSql(String cachedFieldListSql) {
        this.cachedFieldListSql = cachedFieldListSql;
    }

    public String getCachedPrimaryKeys() {
        return cachedPrimaryKeys;
    }

    public void setCachedPrimaryKeys(String cachedPrimaryKeys) {
        this.cachedPrimaryKeys = cachedPrimaryKeys;
    }
}