/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.model.VersionInfo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.manager.ManagerFactory;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.command.impl.PreloadCommand;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.enums.GroupStrategyEnum;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.impl.OperationTemplate;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Group;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.OperationConfig;
import org.dbsyncer.parser.util.ConnectorInstanceUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.storage.StorageService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;

import java.sql.Timestamp;
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

    /**
     * 版本信息
     */
    public static final String DBS_VERSION_INFO = "versionInfo";

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
        Arrays.stream(CommandEnum.values()).filter(CommandEnum::isPreload).forEach(this::execute);

        // Load plugins
        pluginFactory.loadPlugins();

        // Load connectorInstances
        loadConnectorInstance();

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
        // 版本信息检查
        Map versionInfo = map.get(DBS_VERSION_INFO);
        Assert.isTrue(versionInfo != null, "不支持导入低版本或配置不完整");
        VersionInfo info = JsonUtil.jsonToObj(versionInfo.toString(), VersionInfo.class);
        logger.info("upload config: appName={}, version={}, createTime={}", info.getAppName(), info.getVersion(), DateFormatUtil.timestampToString(new Timestamp(info.getCreateTime())));

        // Load configModels
        Stream.of(CommandEnum.PRELOAD_SYSTEM, CommandEnum.PRELOAD_USER, CommandEnum.PRELOAD_CONNECTOR, CommandEnum.PRELOAD_MAPPING, CommandEnum.PRELOAD_META)
                .forEach(commandEnum->reload(map, commandEnum));

        // Load connectorInstances
        loadConnectorInstance();

        // Launch drivers
        launch();
    }

    private void launch() {
        List<Meta> metas = profileComponent.getMetaAll();
        if (!CollectionUtils.isEmpty(metas)) {
            metas.forEach(m-> {
                try {
                    // 重连
                    Mapping mapping = profileComponent.getMapping(m.getMappingId());
                    reConnect(mapping);

                    // 恢复驱动状态
                    if (MetaEnum.RUNNING.getCode() == m.getState()) {
                        managerFactory.start(mapping);
                    } else if (MetaEnum.STOPPING.getCode() == m.getState()) {
                        managerFactory.changeMetaState(m.getId(), MetaEnum.READY);
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            });
        }
    }

    public void reConnect(Mapping mapping) {
        String sourceInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), mapping.getSourceConnectorId(), ConnectorInstanceUtil.SOURCE_SUFFIX);
        String targetInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), mapping.getTargetConnectorId(), ConnectorInstanceUtil.TARGET_SUFFIX);
        Connector connector = profileComponent.getConnector(mapping.getSourceConnectorId());
        ConnectorInstance instance = connectorFactory.connect(sourceInstanceId, connector.getConfig(), mapping.getSourceDatabase(), mapping.getSourceSchema());
        Assert.notNull(instance, "Source connector instance can not null");
        connector = profileComponent.getConnector(mapping.getTargetConnectorId());
        instance = connectorFactory.connect(targetInstanceId, connector.getConfig(), mapping.getTargetDatabase(), mapping.getTargetSchema());
        Assert.notNull(instance, "Target connector instance can not null");
    }

    private void execute(CommandEnum commandEnum) {
        Query query = new Query();
        query.setType(StorageEnum.CONFIG);
        String modelType = commandEnum.getModelType();
        query.addFilter(ConfigConstant.CONFIG_MODEL_TYPE, modelType);

        int pageNum = 1;
        int pageSize = 20;
        long total = 0;
        for (;;) {
            query.setPageNum(pageNum);
            query.setPageSize(pageSize);
            Paging paging = storageService.query(query);
            List<Map> data = (List<Map>) paging.getData();
            if (CollectionUtils.isEmpty(data)) {
                break;
            }
            data.forEach(map-> {
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
        Group group = JsonUtil.jsonToObj(config.toString(), Group.class);
        if (null == group || group.isEmpty()) {
            return;
        }

        for (String id : group.getIndex()) {
            Map m = map.get(id);
            ConfigModel model = (ConfigModel) commandEnum.getCommandExecutor().execute(new PreloadCommand(profileComponent, m.toString()));
            operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD, commandEnum.getGroupStrategyEnum()));
            // Load tableGroups
            if (CommandEnum.PRELOAD_MAPPING == commandEnum) {
                reload(map, CommandEnum.PRELOAD_TABLE_GROUP, operationTemplate.getGroupId(model, GroupStrategyEnum.PRELOAD_TABLE_GROUP));
            }
        }
    }

    private void loadConnectorInstance() {
        List<Connector> list = profileComponent.getConnectorAll();
        if (!CollectionUtils.isEmpty(list)) {
            list.forEach(connector->generalExecutor.execute(()-> {
                try {
                    ConnectorInstance connectorInstance = connectorFactory.connect(connector.getId(), connector.getConfig(), StringUtil.EMPTY, StringUtil.EMPTY);
                    logger.info("Completed connection {} {}", connector.getConfig().getConnectorType(), connectorInstance.getServiceUrl());
                } catch (Exception e) {
                    logger.error("连接配置异常", e);
                    logService.log(LogType.ConnectorLog.FAILED, e.getMessage());
                }
            }));
        }
    }
}