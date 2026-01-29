/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector;

import org.dbsyncer.sdk.enums.TableTypeEnum;
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
    private List<Table> tablePatterns;

    private String mappingId;
    private String connectorId;
    private String suffix;

    public void addTablePattern(String tablePattern) {
        Table table = new Table();
        table.setName(tablePattern);
        table.setType(TableTypeEnum.TABLE.getCode());
        addTablePattern(table);
    }

    public void addTablePattern(Table tablePattern) {
        if (tablePatterns == null) {
            tablePatterns = new ArrayList<>();
        }
        tablePatterns.add(tablePattern);
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
    public List<Table> getTablePatterns() {
        return tablePatterns;
    }

    public void setTablePatterns(List<Table> tablePatterns) {
        this.tablePatterns = tablePatterns;
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