/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.ConnectorConfigService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ConnectorProfile;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.util.ConnectorInstanceUtil;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.DatabaseMapping;
import org.dbsyncer.sdk.model.DatabaseSyncTask;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/17 23:20
 */
@Service
public class ConnectorConfigServiceImpl extends BaseServiceImpl implements ConnectorConfigService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<String, Boolean> health = new ConcurrentHashMap<>();

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private ConnectorProfile connectorProfile;

    @Resource
    private TaskProfile taskProfile;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private LogService logService;

    @Resource
    private Checker connectorChecker;

    @Resource
    private ClusterService clusterService;

    @Override
    public String add(Map<String, String> params) {
        ConfigModel model = connectorChecker.checkAddConfigModel(params);
        log(LogType.ConnectorLog.INSERT, model);

        return profileComponent.addConfigModel(model);
    }

    @Override
    public String copy(String id) {
        Connector connector = profileComponent.getConnector(id);
        Assert.notNull(connector, "The connector id is invalid.");

        ConnectorConfig config = connector.getConfig();
        Map params = JsonUtil.parseMap(config);
        params.put("properties", config.getPropertiesText());
        params.put("extInfo", JsonUtil.objToJson(config.getExtInfo()));
        // 展平 extInfo，供各 ConfigValidator 按表单字段读取（如 pluginName）
        if (config.getExtInfo() != null) {
            for (String key : config.getExtInfo().stringPropertyNames()) {
                String value = config.getExtInfo().getProperty(key);
                if (value != null && !params.containsKey(key)) {
                    params.put(key, value);
                }
            }
        }
        params.put(ConfigConstant.CONFIG_MODEL_NAME, connector.getName() + "(复制)");
        // 与表单开关一致：有值表示开启；复制源开关，避免 checkAdd 缺参断言失败
        if (connector.isSource()) {
            params.put(ConfigConstant.CONNECTOR_IS_SOURCE, "1");
        }
        if (connector.isTarget()) {
            params.put(ConfigConstant.CONNECTOR_IS_TARGET, "1");
        }
        ConfigModel model = connectorChecker.checkAddConfigModel(params);
        log(LogType.ConnectorLog.COPY, model);

        return profileComponent.addConfigModel(model);
    }

    @Override
    public String edit(Map<String, String> params) {
        ConfigModel model = connectorChecker.checkEditConfigModel(params);
        log(LogType.ConnectorLog.UPDATE, model);

        return profileComponent.editConfigModel(model);
    }

    @Override
    public String remove(String id) {
        assertConnectorNotInUse(id);

        Connector connector = profileComponent.getConnector(id);
        if (connector != null) {
            connectorFactory.disconnect(connector.getId());
            log(LogType.ConnectorLog.DELETE, connector);
            profileComponent.removeConfigModel(id);
        }
        return "删除连接器成功!";
    }

    /**
     * 删除前校验：同步驱动、订正校验、整库迁移任务均不可仍引用该连接器。
     */
    private void assertConnectorNotInUse(String id) {
        taskProfile.pageScanTasks(Mapping.class, ConfigConstant.PAGE_SIZE, mappingAll -> {
            if (CollectionUtils.isEmpty(mappingAll)) {
                return;
            }
            for (Mapping mapping : mappingAll) {
                if (mapping == null) {
                    continue;
                }
                if (StringUtil.equals(mapping.getSourceConnectorId(), id) || StringUtil.equals(mapping.getTargetConnectorId(), id)) {
                    String error = String.format("驱动“%s”正在使用，请先删除", mapping.getName());
                    logger.error(error);
                    throw new BizException(error);
                }
            }
        });
        taskProfile.pageScanTasks(ValidateSyncTask.class, ConfigConstant.PAGE_SIZE, tasks -> {
            if (CollectionUtils.isEmpty(tasks)) {
                return;
            }
            for (ValidateSyncTask task : tasks) {
                if (task == null) {
                    continue;
                }
                if (StringUtil.equals(task.getSourceConnectorId(), id) || StringUtil.equals(task.getTargetConnectorId(), id)) {
                    String error = String.format("订正校验任务“%s”正在使用，请先删除", task.getName());
                    logger.error(error);
                    throw new BizException(error);
                }
            }
        });
        taskProfile.pageScanTasks(DatabaseSyncTask.class, ConfigConstant.PAGE_SIZE, tasks -> {
            if (CollectionUtils.isEmpty(tasks)) {
                return;
            }
            for (DatabaseSyncTask task : tasks) {
                if (task == null || CollectionUtils.isEmpty(task.getDatabaseMappings())) {
                    continue;
                }
                for (DatabaseMapping mapping : task.getDatabaseMappings()) {
                    if (mapping == null) {
                        continue;
                    }
                    if (StringUtil.equals(mapping.getSourceConnectorId(), id) || StringUtil.equals(mapping.getTargetConnectorId(), id)) {
                        String error = String.format("整库迁移任务“%s”正在使用，请先删除", task.getName());
                        logger.error(error);
                        throw new BizException(error);
                    }
                }
            }
        });
    }

    @Override
    public Connector getConnector(String id) {
        return profileComponent.getConnector(id);
    }

    @Override
    public List<String> getDatabase(String id) {
        Connector connector = profileComponent.getConnector(id);
        return connector != null ? connector.getDatabases() : Collections.emptyList();
    }

    @Override
    public List<String> getSchema(String id, String database) {
        Connector connector = profileComponent.getConnector(id);
        if (connector == null) {
            return Collections.emptyList();
        }
        ConnectorConfig config = connector.getConfig();
        ConnectorService connectorService = connectorFactory.getConnectorService(config.getConnectorType());
        String catalog = StringUtil.getIfBlank(database, StringUtil.EMPTY);
        try {
            ConnectorInstance connectorInstance = connectorFactory.connect(connector.getId(), config, catalog, StringUtil.EMPTY);
            return connectorService.getSchemas(connectorInstance, database);
        } finally {
            releaseIdleConnector(id);
        }
    }

    @Override
    public List<Connector> getConnectorAll() {
        return profileComponent.getConnectorAll().stream().sorted(Comparator.comparing(Connector::getUpdateTime).reversed()).collect(Collectors.toList());
    }

    @Override
    public Paging<Connector> search(Map<String, String> params) {
        int pageNum = NumberUtil.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtil.toInt(params.get("pageSize"), 10);
        String searchKey = params.get("searchKey");
        // 过滤源库或目标库类型
        String role = params.get("role");
        Paging<Connector> paging = connectorProfile.queryConnectors(pageNum, pageSize, searchKey, role);
        if (paging == null) {
            return null;
        }
        // 整库迁移场景，暂仅支持关系性数据库
        if (StringUtil.equals("1", params.get("relationOnly")) && !CollectionUtils.isEmpty(paging.getData())) {
            paging.setData(paging.getData().stream().filter(this::isRelationalDatabaseConnector).collect(Collectors.toList()));
        }
        return paging;
    }

    @Override
    public List<String> getConnectorTypeAll() {
        ArrayList<String> connectorTypes = new ArrayList<>(connectorFactory.getConnectorTypeAll());
        connectorTypes.sort(Comparator.comparing(String::toString));
        return connectorTypes;
    }

    @Override
    public void refreshHealth() {
        List<Connector> list = profileComponent.getConnectorAll();
        if (CollectionUtils.isEmpty(list)) {
            if (!CollectionUtils.isEmpty(health)) {
                health.clear();
            }
            return;
        }

        // 仅探测已缓存实例，避免集群下为健康检查全量建连
        Set<String> exist = new HashSet<>();
        for (Connector connector : list) {
            exist.add(connector.getId());
            if (!connectorFactory.containsConnector(connector.getId())) {
                continue;
            }
            boolean alive = probeAlive(connector.getId(), connector.getConfig());
            health.put(connector.getId(), alive);
            persistStatusIfChanged(connector, alive);
        }
        // 移除已删除连接器的缓存
        health.keySet().removeIf(id -> !exist.contains(id));
    }

    /**
     * 健康状态变化时回写连接器 {@code STATUS}（含 JSON），未变化则跳过，避免每拍写库。
     */
    private void persistStatusIfChanged(Connector connector, boolean alive) {
        int newStatus = alive ? 1 : 0;
        if (connector.getStatus() == newStatus) {
            return;
        }
        connector.setStatus(newStatus);
        connector.setUpdateTime(System.currentTimeMillis());
        try {
            profileComponent.editConfigModel(connector);
        } catch (Exception e) {
            logger.warn("更新连接器状态失败, connectorId={}, status={}, err={}", connector.getId(), newStatus, e.getMessage());
        }
    }

    @Override
    public boolean isAlive(String id) {
        Connector connector = profileComponent.getConnector(id);
        if (connector == null || connector.getConfig() == null) {
            return false;
        }
        boolean created = false;
        try {
            if (!connectorFactory.containsConnector(id)) {
                connectorFactory.connect(id, connector.getConfig(), StringUtil.EMPTY, StringUtil.EMPTY);
                created = true;
            }
            boolean alive = probeAlive(id, connector.getConfig());
            health.put(id, alive);
            persistStatusIfChanged(connector, alive);
            return alive;
        } catch (Exception e) {
            LogType.ConnectorLog logType = LogType.ConnectorLog.FAILED;
            logService.log(logType, "%s%s", logType.getName(), e.getMessage());
            return false;
        } finally {
            if (created) {
                releaseIdleConnector(id);
            }
        }
    }

    @Override
    public Object getPosition(String mappingId) {
        Mapping mapping = profileComponent.getMapping(mappingId);
        Assert.notNull(mapping, "Mapping can not be null.");
        String instanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), mapping.getSourceConnectorId(), ConnectorInstanceUtil.SOURCE_SUFFIX);
        ConnectorInstance connectorInstance;
        if (connectorFactory.contains(instanceId)) {
            connectorInstance = connectorFactory.connect(instanceId);
        } else {
            Connector connector = profileComponent.getConnector(mapping.getSourceConnectorId());
            Assert.notNull(connector, "源连接器不存在");
            connectorInstance = connectorFactory.connect(instanceId, connector.getConfig(), mapping.getSourceDatabase(), mapping.getSourceSchema());
        }
        return connectorFactory.getPosition(connectorInstance);
    }

    private boolean probeAlive(String connectorConfigId, ConnectorConfig config) {
        try {
            return connectorFactory.isAlive(connectorConfigId, config);
        } catch (Exception e) {
            LogType.ConnectorLog logType = LogType.ConnectorLog.FAILED;
            logService.log(logType, "%s%s", logType.getName(), e.getMessage());
            return false;
        }
    }

    /**
     * 集群下若无运行任务占用该连接器，则断开配置级缓存。
     *
     * @param connectorId 连接器 ID
     */
    private void releaseIdleConnector(String connectorId) {
        if (clusterService.isStandalone() || connectorFactory.isAcquired(connectorId)) {
            return;
        }
        connectorFactory.disconnect(connectorId);
    }

    /**
     * 仅保留关系型数据库连接器。
     */
    private boolean isRelationalDatabaseConnector(Connector connector) {
        if (connector == null || connector.getConfig() == null || StringUtil.isBlank(connector.getConfig().getConnectorType())) {
            return false;
        }
        try {
            ConnectorService connectorService = connectorFactory.getConnectorService(connector.getConfig().getConnectorType());
            return connectorService instanceof AbstractDatabaseConnector;
        } catch (Exception e) {
            logger.warn("过滤关系型连接器失败, connectorId={}, type={}", connector.getId(), connector.getConfig().getConnectorType(), e);
            return false;
        }
    }

}