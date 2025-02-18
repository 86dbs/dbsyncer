/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.ArrayList;
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
        Connector connector = getConnector(id);
        Assert.notNull(connector, "The connector id is invalid.");

        Map params = JsonUtil.parseMap(connector.getConfig());
        params.put(ConfigConstant.CONFIG_MODEL_NAME, connector.getName() + "(复制)");
        ConfigModel model = connectorChecker.checkAddConfigModel(params);
        log(LogType.ConnectorLog.COPY, model);
        profileComponent.addConfigModel(model);

        return String.format("复制成功[%s]", model.getName());
    }

    @Override
    public String edit(Map<String, String> params) {
        ConfigModel model = connectorChecker.checkEditConfigModel(params);
        log(LogType.ConnectorLog.UPDATE, model);

        return profileComponent.editConfigModel(model);
    }

    @Override
    public String remove(String id) {
        List<Mapping> mappingAll = profileComponent.getMappingAll();
        if (!CollectionUtils.isEmpty(mappingAll)) {
            mappingAll.forEach(mapping -> {
                if (StringUtil.equals(mapping.getSourceConnectorId(), id) || StringUtil.equals(mapping.getTargetConnectorId(), id)) {
                    String error = String.format("驱动“%s”正在使用，请先删除", mapping.getName());
                    logger.error(error);
                    throw new BizException(error);
                }
            });
        }

        Connector connector = profileComponent.getConnector(id);
        if (connector != null) {
            connectorFactory.disconnect(connector.getConfig());
            log(LogType.ConnectorLog.DELETE, connector);
            profileComponent.removeConfigModel(id);
        }
        return "删除连接器成功!";
    }

    @Override
    public Connector getConnector(String id) {
        return StringUtil.isNotBlank(id) ? profileComponent.getConnector(id) : null;
    }

    @Override
    public List<Connector> getConnectorAll() {
        return profileComponent.getConnectorAll()
                .stream()
                .sorted(Comparator.comparing(Connector::getUpdateTime).reversed())
                .collect(Collectors.toList());
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
            health.put(c.getId(), isAlive(c.getConfig()));
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
        return health.containsKey(id) && health.get(id);
    }

    @Override
    public Object getPosition(String id) {
        Connector connector = getConnector(id);
        ConnectorInstance connectorInstance = connectorFactory.connect(connector.getConfig());
        return connectorFactory.getPosition(connectorInstance);
    }

    private boolean isAlive(ConnectorConfig config) {
        try {
            return connectorFactory.isAlive(config);
        } catch (Exception e) {
            LogType.ConnectorLog logType = LogType.ConnectorLog.FAILED;
            logService.log(logType, "%s%s", logType.getName(), e.getMessage());
            return false;
        }
    }

}