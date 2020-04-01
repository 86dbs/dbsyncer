/**
 * Alipay.com Inc. Copyright (c) 2004-2020 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.biz.checker.ConnectorConfigChecker;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class ConnectorChecker extends AbstractChecker implements ApplicationContextAware {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    private Map<String, ConnectorConfigChecker> map;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        map = applicationContext.getBeansOfType(ConnectorConfigChecker.class);
    }

    @Override
    public String checkConfigModel(Map<String, String> params) {
        logger.info("check connector params:{}", params);
        Assert.notEmpty(params, "ConnectorChecker check params is null.");
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Connector connector = manager.getConnector(id);
        Assert.notNull(connector, "Can not find connector.");

        // 修改基本配置
        this.modifyConfigModel(connector, params);

        // 配置连接器配置
        ConnectorConfig config = connector.getConfig();
        String type = toLowerCaseFirstOne(config.getConnectorType()) + "ConfigChecker";
        ConnectorConfigChecker checker = map.get(type);
        Assert.notNull(checker, "Checker can not be null.");
        checker.modify(connector, params);

        return JsonUtil.objToJson(connector);
    }

    /**
     * 首字母转小写
     *
     * @param s
     * @return
     */
    private String toLowerCaseFirstOne(String s) {
        if (StringUtils.isBlank(s) || Character.isLowerCase(s.charAt(0))) {
            return s;
        }
        return new StringBuilder().append(Character.toLowerCase(s.charAt(0))).append(s.substring(1)).toString();
    }

}