/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.biz.vo.ConnectorVO;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
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
public class ConnectorServiceImpl extends BaseServiceImpl implements ConnectorService {

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
        org.dbsyncer.sdk.spi.ConnectorService connectorService = connectorFactory.getConnectorService(config.getConnectorType());
        String catalog = StringUtil.getIfBlank(database, StringUtil.EMPTY);
        ConnectorInstance connectorInstance = connectorFactory.connect(connector.getId(), config, catalog, StringUtil.EMPTY);
        return connectorService.getSchemas(connectorInstance, database);
    }

    @Override
    public List<ConnectorVO> getConnectorAll() {
        return profileComponent.getConnectorAll().stream().map(this::convertConnector2Vo).sorted(Comparator.comparing(Connector::getUpdateTime).reversed()).collect(Collectors.toList());
    }

    @Override
    public List<ConnectorVO> getConnectorRelation() {
        return profileComponent.getConnectorAll().stream()
                .filter(this::isRelationalDatabaseConnector)
                .map(this::convertConnector2Vo)
                .sorted(Comparator.comparing(Connector::getUpdateTime).reversed())
                .collect(Collectors.toList());
    }

    @Override
    public Paging<ConnectorVO> search(Map<String, String> params) {
        int pageNum = NumberUtil.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtil.toInt(params.get("pageSize"), 10);
        String searchKey = params.get("searchKey");
        boolean relationOnly = StringUtil.equals("1", params.get("relationOnly"));
        String role = params.get("role");
        Paging<Connector> paging = connectorProfile.queryConnectors(pageNum, pageSize, searchKey, role);
        Paging<ConnectorVO> result = new Paging<>(pageNum, pageSize);
        if (paging == null) {
            return result;
        }
        result.setTotal(paging.getTotal());
        if (CollectionUtils.isEmpty(paging.getData())) {
            return result;
        }
        List<ConnectorVO> rows = new ArrayList<>(paging.getData().size());
        for (Connector connector : paging.getData()) {
            if (connector == null) {
                continue;
            }
            if (relationOnly && !isRelationalDatabaseConnector(connector)) {
                continue;
            }
            rows.add(convertConnector2Vo(connector));
        }
        result.setData(rows);
        return result;
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

        // 更新连接器状态
        Set<String> exist = new HashSet<>();
        list.forEach(c -> {
            health.put(c.getId(), isAlive(c.getId(), c.getConfig()));
            exist.add(c.getId());
        });

        // 移除删除的连接器
        Set<String> remove = new HashSet<>();
        for (Map.Entry<String, Boolean> entry : health.entrySet()) {
            if (!exist.contains(entry.getKey())) {
                remove.add(entry.getKey());
            }
        }

        if (!CollectionUtils.isEmpty(remove)) {
            remove.forEach(health::remove);
        }
    }

    @Override
    public boolean isAlive(String id) {
        return health.getOrDefault(id, false);
    }

    @Override
    public Object getPosition(String mappingId) {
        Mapping mapping = profileComponent.getMapping(mappingId);
        String instanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), mapping.getSourceConnectorId(), ConnectorInstanceUtil.SOURCE_SUFFIX);
        ConnectorInstance connectorInstance = connectorFactory.connect(instanceId);
        return connectorFactory.getPosition(connectorInstance);
    }

    private boolean isAlive(String connectorConfigId, ConnectorConfig config) {
        try {
            return connectorFactory.isAlive(connectorConfigId, config);
        } catch (Exception e) {
            LogType.ConnectorLog logType = LogType.ConnectorLog.FAILED;
            logService.log(logType, "%s%s", logType.getName(), e.getMessage());
            return false;
        }
    }

    /**
     * 仅保留关系型数据库连接器。
     */
    private boolean isRelationalDatabaseConnector(Connector connector) {
        if (connector == null || connector.getConfig() == null || StringUtil.isBlank(connector.getConfig().getConnectorType())) {
            return false;
        }
        try {
            org.dbsyncer.sdk.spi.ConnectorService connectorService = connectorFactory.getConnectorService(connector.getConfig().getConnectorType());
            return connectorService instanceof AbstractDatabaseConnector;
        } catch (Exception e) {
            logger.warn("过滤关系型连接器失败, connectorId={}, type={}", connector.getId(), connector.getConfig().getConnectorType(), e);
            return false;
        }
    }

    private ConnectorVO convertConnector2Vo(Connector connector) {
        ConnectorVO vo = new ConnectorVO(isAlive(connector.getId()));
        BeanUtils.copyProperties(connector, vo);
        return vo;
    }
}