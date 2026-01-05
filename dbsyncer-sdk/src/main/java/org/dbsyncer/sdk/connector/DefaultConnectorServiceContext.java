/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-10-25 16:34
 */
public class DefaultConnectorServiceContext implements ConnectorServiceContext {

    /**
     * must match the catalog name as it is stored in the database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search
     */
    private String catalog;

    /**
     * must match the schema name as it is stored in the database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search
     */
    private String schema;

    /**
     * must match the table name as it is stored in the database
     */
    private List<String> tablePatterns;

    private List<Table> customTablePatterns;

    private String mappingId;
    private String connectorId;
    private String suffix;

    public DefaultConnectorServiceContext(String catalog, String schema, String tablePattern) {
        this.catalog = catalog;
        this.schema = schema;
        this.tablePatterns = new ArrayList<>();
        if (StringUtil.isNotBlank(tablePattern)) {
            this.tablePatterns.add(tablePattern);
        }
    }

    @Override
    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public List<String> getTablePatterns() {
        return tablePatterns;
    }

    public void setTablePatterns(List<String> tablePatterns) {
        this.tablePatterns = tablePatterns;
    }

    @Override
    public List<Table> getCustomTablePatterns() {
        return customTablePatterns;
    }

    public void setCustomTablePatterns(List<Table> customTablePatterns) {
        this.customTablePatterns = customTablePatterns;
    }

    public String getMappingId() {
        return mappingId;
    }

    public void setMappingId(String mappingId) {
        this.mappingId = mappingId;
    }

    public String getConnectorId() {
        return connectorId;
    }

    public void setConnectorId(String connectorId) {
        this.connectorId = connectorId;
    }

    public String getSuffix() {
        return suffix;
    }

    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }
}