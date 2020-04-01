package org.dbsyncer.biz.impl;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/17 23:20
 */
@Service
public class ConnectorServiceImpl implements ConnectorService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    @Autowired
    private Checker connectorChecker;

    @Override
    public boolean alive(String json) {
        logger.info("alive:{}", json);
        return manager.alive(json);
    }

    @Override
    public String add(String json) {
        logger.info("add:{}", json);
        return manager.addConnector(json);
    }

    @Override
    public String edit(Map<String, String> params) {
        String json = connectorChecker.checkConfigModel(params);
        return manager.editConnector(json);
    }

    @Override
    public boolean remove(String id) {
        logger.info("remove:{}", id);
        manager.removeConnector(id);
        return true;
    }

    @Override
    public Connector getConnector(String id) {
        return StringUtils.isNotBlank(id) ? manager.getConnector(id) : null;
    }

    @Override
    public List<Connector> getConnectorAll() {
        return manager.getConnectorAll();
    }
}