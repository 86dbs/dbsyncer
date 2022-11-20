package org.dbsyncer.manager;

import org.dbsyncer.common.event.ClosedEvent;
import org.dbsyncer.common.model.AbstractConnectorConfig;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.listener.enums.QuartzFilterEnum;
import org.dbsyncer.manager.enums.CommandEnum;
import org.dbsyncer.manager.enums.GroupStrategyEnum;
import org.dbsyncer.manager.model.OperationConfig;
import org.dbsyncer.manager.model.QueryConfig;
import org.dbsyncer.manager.template.OperationTemplate;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.enums.ModelEnum;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.plugin.config.Plugin;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
@Component
public class ManagerFactory implements Manager, ApplicationListener<ClosedEvent> {

    @Autowired
    private Parser parser;

    @Autowired
    private PluginFactory pluginFactory;

    @Autowired
    private OperationTemplate operationTemplate;

    @Autowired
    private StorageService storageService;

    @Autowired
    private LogService logService;

    @Autowired
    private Map<String, Puller> map;

    @Override
    public String addUserConfig(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD));
    }

    @Override
    public String editUserConfig(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_EDIT));
    }

    @Override
    public UserConfig getUserConfig(String id) {
        return operationTemplate.queryObject(UserConfig.class, id);
    }

    @Override
    public List<UserConfig> getUserConfigAll() {
        UserConfig userConfig = new UserConfig();
        userConfig.setType(ConfigConstant.USER);
        QueryConfig<UserConfig> queryConfig = new QueryConfig<>(userConfig);
        List<UserConfig> userConfigs = operationTemplate.queryAll(queryConfig);
        return userConfigs;
    }

    @Override
    public String addProjectGroup(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD));
    }

    @Override
    public String editProjectGroup(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_EDIT));
    }

    @Override
    public ProjectGroup getProjectGroup(String id) {
        return operationTemplate.queryObject(ProjectGroup.class, id);
    }

    @Override
    public void removeProjectGroup(String id) {
        operationTemplate.remove(new OperationConfig(id));
    }

    @Override
    public List<ProjectGroup> getProjectGroupAll() {
        ProjectGroup projectGroup = new ProjectGroup();
        projectGroup.setType(ConfigConstant.PROJECT_GROUP);
        QueryConfig<ProjectGroup> queryConfig = new QueryConfig<>(projectGroup);
        List<ProjectGroup> groups = operationTemplate.queryAll(queryConfig);
        return groups;
    }

    @Override
    public ConnectorMapper connect(AbstractConnectorConfig config) {
        return parser.connect(config);
    }

    @Override
    public boolean refreshConnectorConfig(AbstractConnectorConfig config) {
        return parser.refreshConnectorConfig(config);
    }

    @Override
    public boolean isAliveConnectorConfig(AbstractConnectorConfig config) {
        return parser.isAliveConnectorConfig(config);
    }

    @Override
    public List<Table> getTable(ConnectorMapper config) {
        return parser.getTable(config);
    }

    @Override
    public MetaInfo getMetaInfo(String connectorId, String tableName) {
        return parser.getMetaInfo(connectorId, tableName);
    }

    @Override
    public String addConnector(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD));
    }

    @Override
    public String editConnector(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_EDIT));
    }

    @Override
    public void removeConnector(String connectorId) {
        operationTemplate.remove(new OperationConfig(connectorId));
    }

    @Override
    public Connector getConnector(String connectorId) {
        return operationTemplate.queryObject(Connector.class, connectorId);
    }

    @Override
    public List<Connector> getConnectorAll() {
        Connector connector = new Connector();
        connector.setType(ConfigConstant.CONNECTOR);
        QueryConfig<Connector> queryConfig = new QueryConfig<>(connector);
        List<Connector> connectors = operationTemplate.queryAll(queryConfig);
        return connectors;
    }

    @Override
    public void checkAllConnectorStatus() {
        List<Connector> list = getConnectorAll();
        if (!CollectionUtils.isEmpty(list)) {
            list.forEach(c -> {
                try {
                    refreshConnectorConfig(c.getConfig());
                } catch (Exception e) {
                    LogType.ConnectorLog logType = LogType.ConnectorLog.FAILED;
                    logService.log(logType, "%s%s", logType.getName(), e.getMessage());
                }
            });
        }
    }

    @Override
    public String addMapping(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD));
    }

    @Override
    public String editMapping(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_EDIT));
    }

    @Override
    public void removeMapping(String mappingId) {
        operationTemplate.remove(new OperationConfig(mappingId));
    }

    @Override
    public Mapping getMapping(String mappingId) {
        return operationTemplate.queryObject(Mapping.class, mappingId);
    }

    @Override
    public List<Mapping> getMappingAll() {
        Mapping mapping = new Mapping();
        mapping.setType(ConfigConstant.MAPPING);
        QueryConfig<Mapping> queryConfig = new QueryConfig<>(mapping);
        List<Mapping> mappings = operationTemplate.queryAll(queryConfig);
        return mappings;
    }

    @Override
    public String addTableGroup(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD, GroupStrategyEnum.TABLE));
    }

    @Override
    public String editTableGroup(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_EDIT, GroupStrategyEnum.TABLE));
    }

    @Override
    public void removeTableGroup(String tableGroupId) {
        operationTemplate.remove(new OperationConfig(tableGroupId, GroupStrategyEnum.TABLE));
    }

    @Override
    public TableGroup getTableGroup(String tableGroupId) {
        return operationTemplate.queryObject(TableGroup.class, tableGroupId);
    }

    @Override
    public List<TableGroup> getTableGroupAll(String mappingId) {
        TableGroup tableGroup = new TableGroup();
        tableGroup.setType(ConfigConstant.TABLE_GROUP);
        tableGroup.setMappingId(mappingId);
        QueryConfig<TableGroup> queryConfig = new QueryConfig<>(tableGroup, GroupStrategyEnum.TABLE);
        List<TableGroup> tableGroups = operationTemplate.queryAll(queryConfig);
        return tableGroups;
    }

    @Override
    public List<TableGroup> getSortedTableGroupAll(String mappingId) {
        List<TableGroup> list = getTableGroupAll(mappingId)
                .stream()
                .sorted(Comparator.comparing(TableGroup::getIndex).reversed())
                .collect(Collectors.toList());
        return list;
    }

    @Override
    public int getTableGroupCount(String mappingId) {
        TableGroup tableGroup = new TableGroup();
        tableGroup.setType(ConfigConstant.TABLE_GROUP);
        tableGroup.setMappingId(mappingId);
        QueryConfig queryConfig = new QueryConfig<>(tableGroup, GroupStrategyEnum.TABLE);
        return operationTemplate.queryCount(queryConfig);
    }

    @Override
    public Map<String, String> getCommand(Mapping mapping, TableGroup tableGroup) {
        return parser.getCommand(mapping, tableGroup);
    }

    @Override
    public long getCount(String connectorId, Map<String, String> command) {
        return parser.getCount(connectorId, command);
    }

    @Override
    public String addMeta(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD));
    }

    @Override
    public String editMeta(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_EDIT));
    }

    @Override
    public Meta getMeta(String metaId) {
        return operationTemplate.queryObject(Meta.class, metaId);
    }

    @Override
    public void removeMeta(String metaId) {
        operationTemplate.remove(new OperationConfig(metaId));
    }

    @Override
    public List<Meta> getMetaAll() {
        Meta meta = new Meta();
        meta.setType(ConfigConstant.META);
        QueryConfig<Meta> queryConfig = new QueryConfig<>(meta);
        return operationTemplate.queryAll(queryConfig);
    }

    @Override
    public String addSystemConfig(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD));
    }

    @Override
    public String editSystemConfig(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_EDIT));
    }

    @Override
    public SystemConfig getSystemConfig(String configId) {
        return operationTemplate.queryObject(SystemConfig.class, configId);
    }

    @Override
    public List<SystemConfig> getSystemConfigAll() {
        SystemConfig systemConfig = new SystemConfig();
        systemConfig.setType(ConfigConstant.SYSTEM);
        QueryConfig<SystemConfig> queryConfig = new QueryConfig<>(systemConfig);
        return operationTemplate.queryAll(queryConfig);
    }

    @Override
    public Paging queryData(Query query, String collectionId) {
        query.setType(StorageEnum.DATA);
        query.setCollection(collectionId);
        return storageService.query(query);
    }

    @Override
    public void clearData(String collectionId) {
        Meta meta = getMeta(collectionId);
        Mapping mapping = getMapping(meta.getMappingId());
        String model = ModelEnum.getModelEnum(mapping.getModel()).getName();
        LogType.MappingLog log = LogType.MappingLog.CLEAR_DATA;
        logService.log(log, "%s:%s(%s)", log.getMessage(), mapping.getName(), model);
        storageService.clear(StorageEnum.DATA, collectionId);
    }

    @Override
    public Paging queryLog(Query query) {
        query.setType(StorageEnum.LOG);
        return storageService.query(query);
    }

    @Override
    public void clearLog() {
        storageService.clear(StorageEnum.LOG, null);
    }

    @Override
    public List<ConnectorEnum> getConnectorEnumAll() {
        return parser.getConnectorEnumAll();
    }

    @Override
    public List<OperationEnum> getOperationEnumAll() {
        return parser.getOperationEnumAll();
    }

    @Override
    public List<QuartzFilterEnum> getQuartzFilterEnumAll() {
        return parser.getQuartzFilterEnumAll();
    }

    @Override
    public List<FilterEnum> getFilterEnumAll() {
        return parser.getFilterEnumAll();
    }

    @Override
    public List<ConvertEnum> getConvertEnumAll() {
        return parser.getConvertEnumAll();
    }

    @Override
    public List<StorageDataStatusEnum> getStorageDataStatusEnumAll() {
        return parser.getStorageDataStatusEnumAll();
    }

    @Override
    public List<Plugin> getPluginAll() {
        return pluginFactory.getPluginAll();
    }

    @Override
    public String getPluginPath() {
        return pluginFactory.getPluginPath();
    }

    @Override
    public String getLibraryPath() {
        return pluginFactory.getLibraryPath();
    }

    @Override
    public void loadPlugins() {
        pluginFactory.loadPlugins();
    }

    @Override
    public void start(Mapping mapping) {
        Puller puller = getPuller(mapping);

        // 标记运行中
        changeMetaState(mapping.getMetaId(), MetaEnum.RUNNING);

        puller.start(mapping);
    }

    @Override
    public void close(Mapping mapping) {
        Puller puller = getPuller(mapping);

        // 标记停止中
        String metaId = mapping.getMetaId();
        changeMetaState(metaId, MetaEnum.STOPPING);

        puller.close(metaId);
    }

    @Override
    public void changeMetaState(String metaId, MetaEnum metaEnum) {
        Meta meta = getMeta(metaId);
        int code = metaEnum.getCode();
        if (null != meta && meta.getState() != code) {
            meta.setState(code);
            meta.setUpdateTime(Instant.now().toEpochMilli());
            editMeta(meta);
        }
    }

    @Override
    public void onApplicationEvent(ClosedEvent event) {
        // 异步监听任务关闭事件
        changeMetaState(event.getId(), MetaEnum.READY);
    }

    private Puller getPuller(Mapping mapping) {
        Assert.notNull(mapping, "驱动不能为空");
        String model = mapping.getModel();
        String metaId = mapping.getMetaId();
        Assert.hasText(model, "同步方式不能为空");
        Assert.hasText(metaId, "任务ID不能为空");

        Puller puller = map.get(model.concat("Puller"));
        Assert.notNull(puller, String.format("未知的同步方式: %s", model));
        return puller;
    }

}