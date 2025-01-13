/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.QuartzFilterEnum;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.enums.GroupStrategyEnum;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.OperationConfig;
import org.dbsyncer.parser.model.ProjectGroup;
import org.dbsyncer.parser.model.QueryConfig;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-13 21:16
 */
@Component
public class ProfileComponentImpl implements ProfileComponent {

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private ConnectorFactory connectorFactory;

    @Override
    public Connector parseConnector(String json) {
        Map conn = JsonUtil.parseMap(json);
        Map config = (Map) conn.remove("config");
        Connector connector = JsonUtil.jsonToObj(conn.toString(), Connector.class);
        Assert.notNull(connector, "Connector can not be null.");
        String connectorType = (String) config.get("connectorType");
        ConnectorService connectorService = connectorFactory.getConnectorService(connectorType);
        Class<ConnectorConfig> configClass = connectorService.getConfigClass();
        connector.setConfig(JsonUtil.jsonToObj(config.toString(), configClass));

        return connector;
    }

    @Override
    public <T> T parseObject(String json, Class<T> clazz) {
        return JsonUtil.jsonToObj(json, clazz);
    }

    @Override
    public String addConfigModel(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD));
    }

    @Override
    public String editConfigModel(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_EDIT));
    }

    @Override
    public void removeConfigModel(String id) {
        operationTemplate.remove(new OperationConfig(id));
    }

    @Override
    public SystemConfig getSystemConfig() {
        List<SystemConfig> list = operationTemplate.queryAll(SystemConfig.class);
        return CollectionUtils.isEmpty(list) ? null : list.get(0);
    }

    @Override
    public UserConfig getUserConfig() {
        List<UserConfig> list = operationTemplate.queryAll(UserConfig.class);
        return CollectionUtils.isEmpty(list) ? null : list.get(0);
    }

    @Override
    public ProjectGroup getProjectGroup(String id) {
        return operationTemplate.queryObject(ProjectGroup.class, id);
    }

    @Override
    public List<ProjectGroup> getProjectGroupAll() {
        return operationTemplate.queryAll(ProjectGroup.class);
    }

    @Override
    public Connector getConnector(String connectorId) {
        return operationTemplate.queryObject(Connector.class, connectorId);
    }

    @Override
    public List<Connector> getConnectorAll() {
        return operationTemplate.queryAll(Connector.class);
    }

    @Override
    public Mapping getMapping(String mappingId) {
        return operationTemplate.queryObject(Mapping.class, mappingId);
    }

    @Override
    public List<Mapping> getMappingAll() {
        return operationTemplate.queryAll(Mapping.class);
    }

    @Override
    public String addTableGroup(TableGroup model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD, GroupStrategyEnum.TABLE));
    }

    @Override
    public String editTableGroup(TableGroup model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_EDIT, GroupStrategyEnum.TABLE));
    }

    @Override
    public void removeTableGroup(String id) {
        operationTemplate.remove(new OperationConfig(id, GroupStrategyEnum.TABLE));
    }

    @Override
    public TableGroup getTableGroup(String tableGroupId) {
        return operationTemplate.queryObject(TableGroup.class, tableGroupId);
    }

    @Override
    public List<TableGroup> getTableGroupAll(String mappingId) {
        TableGroup tableGroup = new TableGroup().setMappingId(mappingId);
        return operationTemplate.queryAll(new QueryConfig(tableGroup, GroupStrategyEnum.TABLE));
    }

    @Override
    public List<TableGroup> getSortedTableGroupAll(String mappingId) {
        return getTableGroupAll(mappingId)
                .stream()
                .sorted(Comparator.comparing(TableGroup::getIndex).reversed())
                .collect(Collectors.toList());
    }

    @Override
    public int getTableGroupCount(String mappingId) {
        TableGroup tableGroup = new TableGroup().setMappingId(mappingId);
        return operationTemplate.queryCount(new QueryConfig<>(tableGroup, GroupStrategyEnum.TABLE));
    }

    @Override
    public Meta getMeta(String metaId) {
        return operationTemplate.queryObject(Meta.class, metaId);
    }

    @Override
    public List<Meta> getMetaAll() {
        return operationTemplate.queryAll(Meta.class);
    }

    @Override
    public List<OperationEnum> getOperationEnumAll() {
        return Arrays.asList(OperationEnum.values());
    }

    @Override
    public List<QuartzFilterEnum> getQuartzFilterEnumAll() {
        return Arrays.asList(QuartzFilterEnum.values());
    }

    @Override
    public List<FilterEnum> getFilterEnumAll() {
        return Arrays.asList(FilterEnum.values());
    }

    @Override
    public List<ConvertEnum> getConvertEnumAll() {
        return Arrays.asList(ConvertEnum.values());
    }

    @Override
    public List<StorageDataStatusEnum> getStorageDataStatusEnumAll() {
        return Arrays.asList(StorageDataStatusEnum.values());
    }

}