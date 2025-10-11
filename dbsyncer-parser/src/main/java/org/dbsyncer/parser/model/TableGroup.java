/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import com.alibaba.fastjson2.annotation.JSONField;
import org.apache.logging.log4j.util.Strings;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @JSONField(serialize = false)
    public static final int Version = 1;
    public int currentVersion;
    @JSONField(serialize = false)
    public boolean isInit = false;

    @JSONField(serialize = false)
    public ParserComponent parserComponent;
    @JSONField(serialize = false)
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
    @JSONField(serialize = false)
    private Map<String, String> command = new HashMap<>();

    // 缓存的字段列表SQL片段（避免重复构建）
    @JSONField(serialize = false)
    private String cachedFieldListSql;

    // 缓存的主键列表字符串（避免重复构建）
    @JSONField(serialize = false)
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

    public void initTableGroup(ParserComponent parserComponent, ProfileComponent profileComponent) {
        if (isInit) return;
        this.parserComponent = parserComponent;
        this.profileComponent = profileComponent;
        if (currentVersion != TableGroup.Version) {
            currentVersion = TableGroup.Version;
            profileComponent.editConfigModel(this);
        }
        Mapping mapping = profileComponent.getMapping(mappingId);
        setCommand(parserComponent.getCommand(mapping, this));
        isInit = true;
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

    @JSONField(serialize = false)
    public boolean hasError() {
        return Strings.isNotBlank(errorMessage);
    }

    /**
     * 清理错误状态
     */
    @JSONField(serialize = false)
    public void clear() {
        fullCompleted = false;
        cursors = null;
        this.errorMessage = null;
    }

    @JSONField(serialize = false)
    public static TableGroup create(String mappingId, String sourceTable, ParserComponent parserComponent, ProfileComponent profileComponent) {
        TableGroup tableGroup = new TableGroup();
        tableGroup.setName(mappingId + ":" + sourceTable);
        tableGroup.currentVersion = TableGroup.Version;
        tableGroup.parserComponent = parserComponent;
        tableGroup.profileComponent = profileComponent;
        tableGroup.setMappingId(mappingId);
        return tableGroup;
    }


    public TableGroup copy(String mappingId, SnowflakeIdWorker snowflakeIdWorker) {
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