/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Map;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-08 15:17
 */
@Component
public class ConnectorChecker extends AbstractChecker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private ConnectorFactory connectorFactory;

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        printParams(params);
        String name = params.get(ConfigConstant.CONFIG_MODEL_NAME);
        String connectorType = params.get("connectorType");
        Assert.hasText(name, "connector name is empty.");
        Assert.hasText(connectorType, "connector connectorType is empty.");

        Connector connector = new Connector();
        connector.setName(name);
        ConnectorConfig config = getConfig(connectorType);
        connector.setConfig(config);

        // 连接器配置校验
        ConfigValidator configValidator = connectorFactory.getConnectorService(connectorType).getConfigValidator();
        Assert.notNull(configValidator, "ConfigValidator can not be null.");
        configValidator.modify(config, params);

        // 获取表
        ConnectorInstance connectorInstance = connectorFactory.connect(connector.getConfig());
        connector.setTable(connectorFactory.getTable(connectorInstance));

        // 修改基本配置
        this.modifyConfigModel(connector, params);

        return connector;
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) {
        printParams(params);
        Assert.notEmpty(params, "ConnectorChecker check params is null.");
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Connector connector = profileComponent.getConnector(id);
        Assert.notNull(connector, "Can not find connector.");
        ConnectorConfig config = connector.getConfig();
        connectorFactory.disconnect(config);

        // 修改基本配置
        this.modifyConfigModel(connector, params);

        // 连接器配置校验
        ConfigValidator configValidator = connectorFactory.getConnectorService(config.getConnectorType()).getConfigValidator();
        Assert.notNull(configValidator, "ConfigValidator can not be null.");
        configValidator.modify(config, params);

        // 获取表
        ConnectorInstance connectorInstance = connectorFactory.connect(config);
        connector.setTable(connectorFactory.getTable(connectorInstance));

        return connector;
    }

    private ConnectorConfig getConfig(String connectorType) {
        try {
            ConnectorService connectorService = connectorFactory.getConnectorService(connectorType);
            Class<ConnectorConfig> configClass = connectorService.getConfigClass();
            ConnectorConfig config = configClass.newInstance();
            config.setConnectorType(connectorService.getConnectorType());
            return config;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new BizException("获取连接器配置异常.");
        }
    }

}