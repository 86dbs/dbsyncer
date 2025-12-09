/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.logging.log4j.util.Strings;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Filter;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/15 23:56
 */
public class TableGroup extends AbstractConfigModel {

    public TableGroup() {
        super.setType(ConfigConstant.TABLE_GROUP);
        super.setName(ConfigConstant.TABLE_GROUP);
    }

    @JsonIgnore
    public static final int Version = 1;
    public int currentVersion;
    @JsonIgnore
    public boolean isInit = false;

    @JsonIgnore
    public ParserComponent parserComponent;
    @JsonIgnore
    public ProfileComponent profileComponent;

    // 排序索引
    private int index;

    // 驱动映射关系ID
    private String mappingId;

    // 数据源表
    private Table sourceTable;

    // 目标源表
    private Table targetTable;

    // 字段映射关系
    private List<FieldMapping> fieldMapping = new ArrayList<>();

    // 执行命令，例SQL等
    @JsonIgnore
    private Map<String, String> command = new HashMap<>();

    // 缓存的字段列表SQL片段（避免重复构建）
    @JsonIgnore
    private String cachedFieldListSql;

    // 缓存的主键列表字符串（避免重复构建）
    @JsonIgnore
    private String cachedPrimaryKeys;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getMappingId() {
        return mappingId;
    }

    public TableGroup setMappingId(String mappingId) {
        this.mappingId = mappingId;
        return this;
    }

    public Table getSourceTable() {
        return sourceTable;
    }

    public TableGroup setSourceTable(Table sourceTable) {
        this.sourceTable = sourceTable;
        return this;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public TableGroup setTargetTable(Table targetTable) {
        this.targetTable = targetTable;
        return this;
    }

    public List<FieldMapping> getFieldMapping() {
        return fieldMapping;
    }

    public void setFieldMapping(List<FieldMapping> fieldMapping) {
        this.fieldMapping = fieldMapping;
    }

    public Map<String, String> getCommand() {
        return command;
    }

    public TableGroup setCommand(Map<String, String> command) {
        this.command = command;
        return this;
    }

    public String getCachedFieldListSql() {
        return cachedFieldListSql;
    }

    public TableGroup setCachedFieldListSql(String cachedFieldListSql) {
        this.cachedFieldListSql = cachedFieldListSql;
        return this;
    }

    public String getCachedPrimaryKeys() {
        return cachedPrimaryKeys;
    }

    public TableGroup setCachedPrimaryKeys(String cachedPrimaryKeys) {
        this.cachedPrimaryKeys = cachedPrimaryKeys;
        return this;
    }

    @JsonIgnore
    public void initTableGroup(ParserComponent parserComponent, ProfileComponent profileComponent, ConnectorFactory connectorFactory) throws Exception {
        if (isInit) return;
        this.parserComponent = parserComponent;
        this.profileComponent = profileComponent;
        Mapping mapping = profileComponent.getMapping(mappingId);
        initCommand(mapping, connectorFactory);
        if (currentVersion != TableGroup.Version) {
            currentVersion = TableGroup.Version;
            profileComponent.editConfigModel(this);
        }
        isInit = true;
    }

    @JsonIgnore
    public void initCommand(Mapping mapping, ConnectorFactory connectorFactory) throws Exception {
        ConnectorConfig sConnConfig = profileComponent.getConnector(mapping.getSourceConnectorId()).getConfig();
        ConnectorConfig tConnConfig = profileComponent.getConnector(mapping.getTargetConnectorId()).getConfig();
        Table sourceTable = this.getSourceTable();
        Table targetTable = this.getTargetTable();
        Table sTable = sourceTable.clone().setColumn(new ArrayList<>());
        Table tTable = targetTable.clone().setColumn(new ArrayList<>());
        List<FieldMapping> fieldMapping = this.getFieldMapping();
        if (!CollectionUtils.isEmpty(fieldMapping)) {
            fieldMapping.forEach(m -> {
                if (null != m.getSource()) {
                    sTable.getColumn().add(m.getSource());
                }
                if (null != m.getTarget()) {
                    tTable.getColumn().add(m.getTarget());
                }
            });
        }
        // 如果 tableGroup.getFilter()空使用 mapping.getFilter()0
        List<Filter> filters = CollectionUtils.isEmpty(this.getFilter()) ? mapping.getFilter() : this.getFilter();
        final CommandConfig sourceConfig = new CommandConfig(sConnConfig.getConnectorType(), sTable,
                connectorFactory.connect(sConnConfig), filters);
        final CommandConfig targetConfig = new CommandConfig(tConnConfig.getConnectorType(), tTable,
                connectorFactory.connect(tConnConfig), null);

        // 预构建字段列表SQL片段并缓存
        String fieldListSql = this.getCachedFieldListSql();
        if (StringUtil.isBlank(fieldListSql)) {
            ConnectorService connectorService = connectorFactory.getConnectorService(sConnConfig);
            // 使用SQL模板的引号方法
            if (connectorService instanceof AbstractDatabaseConnector) {
                AbstractDatabaseConnector dbConnector = (AbstractDatabaseConnector) connectorService;
                List<String> fieldList = sTable.getColumn().stream().map(Field::getName).collect(Collectors.toList());
                fieldListSql = dbConnector.sqlTemplate.buildQuotedStringList(fieldList);
                this.setCachedFieldListSql(fieldListSql);
                List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(sTable);
                this.setCachedPrimaryKeys(dbConnector.sqlTemplate.buildQuotedStringList(primaryKeys));
            }
        }
        // 将缓存的字段列表设置到CommandConfig中
        sourceConfig.setCachedFieldListSql(fieldListSql);

        // 将缓存的主键列表设置到CommandConfig中
        sourceConfig.setCachedPrimaryKeys(this.getCachedPrimaryKeys());

        // 获取连接器同步参数
        Map<String, String> commandGenerated = connectorFactory.getCommand(sourceConfig, targetConfig);
        this.command = commandGenerated;
    }

    // 新增字段 - 流式处理状态管理
    private Object[] cursors; // 当前TableGroup的cursor
    private boolean fullCompleted; // 流式处理是否完成
    private String errorMessage; // 错误信息

    public Object[] getCursors() {
        return cursors;
    }

    public void setCursors(Object[] cursors) {
        this.cursors = cursors;
    }

    public boolean isFullCompleted() {
        return fullCompleted;
    }

    public void setFullCompleted(boolean fullCompleted) {
        this.fullCompleted = fullCompleted;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @JsonIgnore
    public boolean hasError() {
        return Strings.isNotBlank(errorMessage);
    }

    /**
     * 清理错误状态
     */
    @JsonIgnore
    public void clear() {
        isInit = false;
        fullCompleted = false;
        cursors = null;
        this.errorMessage = null;
    }

    @JsonIgnore
    public static TableGroup create(String mappingId, String sourceTable, ParserComponent parserComponent, ProfileComponent profileComponent) {
        TableGroup tableGroup = new TableGroup();
        tableGroup.setName(mappingId + ":" + sourceTable);
        tableGroup.currentVersion = TableGroup.Version;
        tableGroup.parserComponent = parserComponent;
        tableGroup.profileComponent = profileComponent;
        tableGroup.setMappingId(mappingId);
        return tableGroup;
    }

    @JsonIgnore
    public TableGroup copy(String mappingId, SnowflakeIdWorker snowflakeIdWorker) throws Exception {
        String tableGroupJson = JsonUtil.objToJson(this);
        TableGroup newTableGroup = JsonUtil.jsonToObj(tableGroupJson, TableGroup.class);
        newTableGroup.clear();
        newTableGroup.setId(String.valueOf(snowflakeIdWorker.nextId()));
        newTableGroup.setMappingId(mappingId);
        newTableGroup.setName(mappingId + ":" + newTableGroup.getSourceTable().getName());

        profileComponent.addTableGroup(newTableGroup);
        return newTableGroup;
    }
}