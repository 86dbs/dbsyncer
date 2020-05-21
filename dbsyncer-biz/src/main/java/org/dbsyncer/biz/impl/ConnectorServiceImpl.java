package org.dbsyncer.biz.impl;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    public String add(Map<String, String> params) {
        ConfigModel model = connectorChecker.checkAddConfigModel(params);
        return manager.addConnector(model);
    }

    @Override
    public String edit(Map<String, String> params) {
        ConfigModel model = connectorChecker.checkEditConfigModel(params);
        return manager.editConnector(model);
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

}