package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.CheckService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.biz.checker.impl.MappingChecker;
import org.dbsyncer.biz.checker.impl.MysqlChecker;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/7 22:59
 */
@Service
public class CheckServiceImpl implements CheckService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    private Map<String, Checker> map;

    @PostConstruct
    public void init() {
        map = new LinkedHashMap<>();
        map.put(ConnectorEnum.MYSQL.getType(), new MysqlChecker());
        map.put(ConfigConstant.MAPPING, new MappingChecker());
    }

    @Override
    public String checkConnector(Map<String, String> params) {
        logger.info("check connector params:{}", params);
        Assert.notEmpty(params, "CheckServiceImpl check connector params is null.");
        String id = params.get("id");
        Connector connector = manager.getConnector(id);
        Assert.notNull(connector, "Can not find connector.");
        ConnectorConfig config = connector.getConfig();
        String type = config.getConnectorType();
        Checker checker = map.get(type);
        Assert.notNull(checker, "Checker can not be null.");

        checker.modify(connector, params);
        String json = JsonUtil.objToJson(connector);
        logger.info("check success:{}", json);
        return json;
    }

    @Override
    public String checkMapping(Map<String, String> params) {
        logger.info("check mapping params:{}", params);
        Assert.notEmpty(params, "CheckServiceImpl check mapping params is null.");
        String id = params.get("id");
        Mapping mapping = manager.getMapping(id);
        Assert.notNull(mapping, "Can not find mapping.");
        String type = mapping.getType();
        Checker checker = map.get(type);
        Assert.notNull(checker, "Checker can not be null.");

        checker.modify(mapping, params);
        String json = JsonUtil.objToJson(mapping);
        logger.info("check success:{}", json);
        return json;
    }

}