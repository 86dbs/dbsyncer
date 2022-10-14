package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/17 23:20
 */
@Service
public class ConnectorServiceImpl extends BaseServiceImpl implements ConnectorService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, Boolean> health = new LinkedHashMap<>();

    @Autowired
    private Manager manager;

    @Autowired
    private Checker connectorChecker;

    @Override
    public String add(Map<String, String> params) {
        ConfigModel model = connectorChecker.checkAddConfigModel(params);
        log(LogType.ConnectorLog.INSERT, model);

        return manager.addConnector(model);
    }

    @Override
    public String copy(String id) {
        Connector connector = getConnector(id);
        Assert.notNull(connector, "The connector id is invalid.");

        Map params = JsonUtil.parseObject(JsonUtil.objToJson(connector.getConfig())).getInnerMap();
        params.put(ConfigConstant.CONFIG_MODEL_NAME, connector.getName() + "(复制)");
        ConfigModel model = connectorChecker.checkAddConfigModel(params);
        log(LogType.ConnectorLog.COPY, model);
        manager.addConnector(model);

        return String.format("复制成功[%s]", model.getName());
    }

    @Override
    public String edit(Map<String, String> params) {
        ConfigModel model = connectorChecker.checkEditConfigModel(params);
        log(LogType.ConnectorLog.UPDATE, model);

        return manager.editConnector(model);
    }

    @Override
    public String remove(String id) {
        List<Mapping> mappingAll = manager.getMappingAll();
        if (!CollectionUtils.isEmpty(mappingAll)) {
            mappingAll.forEach(mapping -> {
                if (StringUtil.equals(mapping.getSourceConnectorId(), id) || StringUtil.equals(mapping.getTargetConnectorId(), id)) {
                    String error = String.format("驱动“%s”正在使用，请先删除", mapping.getName());
                    logger.error(error);
                    throw new BizException(error);
                }
            });
        }

        Connector connector = manager.getConnector(id);
        log(LogType.ConnectorLog.DELETE, connector);

        manager.removeConnector(id);
        return "删除连接器成功!";
    }

    @Override
    public Connector getConnector(String id) {
        return StringUtil.isNotBlank(id) ? manager.getConnector(id) : null;
    }

    @Override
    public List<Connector> getConnectorAll() {
        List<Connector> list = manager.getConnectorAll()
                .stream()
                .sorted(Comparator.comparing(Connector::getUpdateTime).reversed())
                .collect(Collectors.toList());
        return list;
    }

    @Override
    public List<String> getConnectorTypeAll() {
        List<String> list = new ArrayList<>();
        manager.getConnectorEnumAll().forEach(c -> list.add(c.getType()));
        return list;
    }

    @Override
    public void refreshHealth() {
        List<Connector> list = manager.getConnectorAll();
        if (CollectionUtils.isEmpty(list)) {
            if (!CollectionUtils.isEmpty(health)) {
                health.clear();
            }
            return;
        }

        // 更新连接器状态
        Set<String> exist = new HashSet<>();
        list.forEach(c -> {
            health.put(c.getId(), manager.isAliveConnectorConfig(c.getConfig()));
            exist.add(c.getId());
        });

        // 移除删除的连接器
        Set<String> remove = new HashSet<>();
        health.keySet().forEach(k -> {
            if (!exist.contains(k)) {
                remove.add(k);
            }
        });

        if (!CollectionUtils.isEmpty(remove)) {
            remove.forEach(k -> health.remove(k));
        }
    }

    @Override
    public boolean isAlive(String id) {
        return health.containsKey(id) && health.get(id);
    }
}