package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.CheckService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/7 22:59
 */
@Service
public class CheckServiceImpl implements CheckService, ApplicationContextAware {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

//    @Override
//    public String checkConnector(Map<String, String> params) {
//        logger.info("check connector params:{}", params);
//        Assert.notEmpty(params, "CheckServiceImpl check connector params is null.");
//        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
//        Connector connector = manager.getConnector(id);
//        Assert.notNull(connector, "Can not find connector.");
//        ConnectorConfig config = connector.getConfig();
//        String type = config.getConnectorType();
//        Checker checker = map.get(type);
//        Assert.notNull(checker, "Checker can not be null.");
//
//        checker.modify(connector, params);
//        String json = JsonUtil.objToJson(connector);
//        logger.info("check success:{}", json);
//        return json;
//    }

//    @Override
//    public String checkMapping(Map<String, String> params) {
//        logger.info("check mapping params:{}", params);
//        Assert.notEmpty(params, "CheckServiceImpl check mapping params is null.");
//        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
//        Mapping mapping = manager.getMapping(id);
//        Assert.notNull(mapping, "Can not find mapping.");
//        String type = mapping.getType();
//        Checker checker = map.get(type);
//        Assert.notNull(checker, "Checker can not be null.");
//
//        checker.modify(mapping, params);
//        String json = JsonUtil.objToJson(mapping);
//        logger.info("check success:{}", json);
//        return json;
//    }

    @Override
    public <T> T check(Map<String, String> params, Class<T> type) {
        T checker = applicationContext.getBean(type);
        Assert.notNull(checker, "Checker can not be null.");

//        checker.modify(null, params);
        return null;
    }
}