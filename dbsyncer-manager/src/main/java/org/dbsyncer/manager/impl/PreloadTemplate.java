package org.dbsyncer.manager.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.manager.ManagerFactory;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.command.PreloadCommand;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.enums.GroupStrategyEnum;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.impl.OperationTemplate;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.OperationConfig;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.sdk.spi.ConnectorMapper;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

/**
 * 预加载配置模板
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
@Component
public final class PreloadTemplate implements ApplicationListener<ContextRefreshedEvent> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private ManagerFactory managerFactory;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private StorageService storageService;

    @Resource
    private LogService logService;

    @Resource
    private Executor generalExecutor;

    private boolean preloadCompleted;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        // Load configModels
        Arrays.stream(CommandEnum.values()).filter(commandEnum -> commandEnum.isPreload()).forEach(commandEnum -> execute(commandEnum));

        // Load plugins
        pluginFactory.loadPlugins();

        // Load connectorMappers
        loadConnectorMapper();

        // Launch drivers
        launch();

        preloadCompleted = true;
    }

    /**
     * 是否完成预加载配置
     *
     * @return
     */
    public boolean isPreloadCompleted() {
        return preloadCompleted;
    }

    public void reload(String json) {
        Map<String, Map> map = JsonUtil.jsonToObj(json, Map.class);
        if (CollectionUtils.isEmpty(map)) {
            return;
        }

        // Load configModels
        Stream.of(CommandEnum.PRELOAD_SYSTEM, CommandEnum.PRELOAD_USER, CommandEnum.PRELOAD_CONNECTOR, CommandEnum.PRELOAD_MAPPING,
                CommandEnum.PRELOAD_META, CommandEnum.PRELOAD_PROJECT_GROUP).forEach(commandEnum -> reload(map, commandEnum));

        // Load connectorMappers
        loadConnectorMapper();

        // Launch drivers
        launch();
    }

    private void launch() {
        List<Meta> metas = profileComponent.getMetaAll();
        if (!CollectionUtils.isEmpty(metas)) {
            metas.forEach(m -> {
                // 恢复驱动状态
                if (MetaEnum.RUNNING.getCode() == m.getState()) {
                    Mapping mapping = profileComponent.getMapping(m.getMappingId());
                    managerFactory.start(mapping);
                } else if (MetaEnum.STOPPING.getCode() == m.getState()) {
                    managerFactory.changeMetaState(m.getId(), MetaEnum.READY);
                }
            });
        }
    }

    private void execute(CommandEnum commandEnum) {
        Query query = new Query();
        query.setType(StorageEnum.CONFIG);
        String modelType = commandEnum.getModelType();
        query.addFilter(ConfigConstant.CONFIG_MODEL_TYPE, modelType);

        int pageNum = 1;
        int pageSize = 20;
        long total = 0;
        for (; ; ) {
            query.setPageNum(pageNum);
            query.setPageSize(pageSize);
            Paging paging = storageService.query(query);
            List<Map> data = (List<Map>) paging.getData();
            if (CollectionUtils.isEmpty(data)) {
                break;
            }
            data.forEach(map -> {
                String json = (String) map.get(ConfigConstant.CONFIG_MODEL_JSON);
                ConfigModel model = (ConfigModel) commandEnum.getCommandExecutor().execute(new PreloadCommand(profileComponent, json));
                if (null != model) {
                    operationTemplate.cache(model, commandEnum.getGroupStrategyEnum());
                }
            });
            total += paging.getTotal();
            pageNum++;
        }
        logger.info("{}:{}", modelType, total);
    }

    private void reload(Map<String, Map> map, CommandEnum commandEnum) {
        reload(map, commandEnum, commandEnum.getModelType());
    }

    private void reload(Map<String, Map> map, CommandEnum commandEnum, String groupId) {
        Map config = map.get(groupId);
        if (null == config) {
            return;
        }
        OperationTemplate.Group group = JsonUtil.jsonToObj(config.toString(), OperationTemplate.Group.class);
        if (null == group) {
            return;
        }

        List<String> index = group.getIndex();
        if (CollectionUtils.isEmpty(index)) {
            return;
        }

        for (String e : index) {
            Map m = map.get(e);
            ConfigModel model = (ConfigModel) commandEnum.getCommandExecutor().execute(new PreloadCommand(profileComponent, m.toString()));
            operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD, commandEnum.getGroupStrategyEnum()));
            // Load tableGroups
            if (CommandEnum.PRELOAD_MAPPING == commandEnum) {
                reload(map, CommandEnum.PRELOAD_TABLE_GROUP, operationTemplate.getGroupId(model, GroupStrategyEnum.PRELOAD_TABLE_GROUP));
            }
        }
    }

    private void loadConnectorMapper() {
        List<Connector> list = profileComponent.getConnectorAll();
        if (!CollectionUtils.isEmpty(list)) {
            list.forEach(connector -> {
                generalExecutor.execute(() -> {
                    try {
                        connectorFactory.disconnect(connector.getConfig());
                        ConnectorMapper mapper = connectorFactory.connect(connector.getConfig());
                        logger.info("Completed connection {} {}", connector.getConfig().getConnectorType(), mapper.getServiceUrl());
                    } catch (Exception e) {
                        logger.error("连接配置异常", e);
                        logService.log(LogType.ConnectorLog.FAILED, e.getMessage());
                    }
                });
            });
        }
    }
}