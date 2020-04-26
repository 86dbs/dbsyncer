package org.dbsyncer.manager;

import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.config.MetaInfo;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.listener.Listener;
import org.dbsyncer.manager.config.OperationConfig;
import org.dbsyncer.manager.config.PreloadConfig;
import org.dbsyncer.manager.config.QueryConfig;
import org.dbsyncer.manager.enums.GroupStrategyEnum;
import org.dbsyncer.manager.enums.HandlerEnum;
import org.dbsyncer.manager.template.impl.OperationTemplate;
import org.dbsyncer.manager.template.impl.PreloadTemplate;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.plugin.config.Plugin;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
@Component
public class ManagerFactory implements Manager, ApplicationContextAware, ApplicationListener<ContextRefreshedEvent> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Parser parser;

    @Autowired
    private PluginFactory pluginFactory;

    @Autowired
    private Listener listener;

    @Autowired
    private PreloadTemplate preloadTemplate;

    @Autowired
    private OperationTemplate operationTemplate;

    private Map<String, Executor> map;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        map = applicationContext.getBeansOfType(Executor.class);
    }

    @Override
    public boolean alive(ConnectorConfig config) {
        return parser.alive(config);
    }

    @Override
    public List<String> getTable(ConnectorConfig config) {
        return parser.getTable(config);
    }

    @Override
    public MetaInfo getMetaInfo(String connectorId, String tableName) {
        return parser.getMetaInfo(connectorId, tableName);
    }

    @Override
    public String addConnector(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, HandlerEnum.OPR_ADD.getHandler()));
    }

    @Override
    public String editConnector(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, HandlerEnum.OPR_EDIT.getHandler()));
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
    public String addMapping(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, HandlerEnum.OPR_ADD.getHandler()));
    }

    @Override
    public String editMapping(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, HandlerEnum.OPR_EDIT.getHandler()));
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
        return operationTemplate.execute(new OperationConfig(model, GroupStrategyEnum.TABLE, HandlerEnum.OPR_ADD.getHandler()));
    }

    @Override
    public String editTableGroup(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, GroupStrategyEnum.TABLE, HandlerEnum.OPR_EDIT.getHandler()));
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
    public Map<String, String> getCommand(String sourceConnectorId, String targetConnectorId, TableGroup tableGroup) {
        return parser.getCommand(sourceConnectorId, targetConnectorId, tableGroup);
    }

    @Override
    public String addMeta(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, HandlerEnum.OPR_ADD.getHandler()));
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
        List<Meta> metas = operationTemplate.queryAll(queryConfig);
        return metas;
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
    public List<FilterEnum> getFilterEnumAll() {
        return parser.getFilterEnumAll();
    }

    @Override
    public List<ConvertEnum> getConvertEnumAll() {
        return parser.getConvertEnumAll();
    }

    @Override
    public List<Plugin> getPluginAll() {
        return pluginFactory.getPluginAll();
    }

    @Override
    public boolean start(Mapping mapping) {
        // 获取数据源连接器
        Connector connector = getConnector(mapping.getSourceConnectorId());
        Assert.notNull(connector, "数据源配置不能为空");

        // 启动任务
        Executor executor = getExecutor(mapping);
        boolean start = executor.start(mapping.getMetaId(), mapping.getListener(), connector.getConfig());
        return start;
    }

    @Override
    public boolean close(Mapping mapping) {
        // 关闭任务
        Executor executor = getExecutor(mapping);
        boolean shutdown = executor.shutdown(mapping.getMetaId());
        return shutdown;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        // Load connectors
        preloadTemplate.execute(new PreloadConfig(ConfigConstant.CONNECTOR, HandlerEnum.PRELOAD_CONNECTOR.getHandler()));
        // Load mappings
        preloadTemplate.execute(new PreloadConfig(ConfigConstant.MAPPING, HandlerEnum.PRELOAD_MAPPING.getHandler()));
        // Load tableGroups
        preloadTemplate.execute(new PreloadConfig(ConfigConstant.TABLE_GROUP, GroupStrategyEnum.TABLE, HandlerEnum.PRELOAD_TABLE_GROUP.getHandler()));
        // Load metas
        preloadTemplate.execute(new PreloadConfig(ConfigConstant.META, HandlerEnum.PRELOAD_META.getHandler()));

    }

    private Executor getExecutor(Mapping mapping) {
        Assert.notNull(mapping, "驱动不能为空");
        String model = mapping.getModel();
        String metaId = mapping.getMetaId();
        Assert.hasText(model, "同步方式不能为空");
        Assert.hasText(metaId, "任务ID不能为空");

        Executor executor = map.get(model.concat("Executor"));
        Assert.notNull(executor, String.format("未知的同步方式: %s", model));
        return executor;
    }

}