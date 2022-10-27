package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.biz.checker.ConnectorConfigChecker;
import org.dbsyncer.common.model.AbstractConnectorConfig;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class ConnectorChecker extends AbstractChecker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    @Autowired
    private LogService logService;

    @Autowired
    private Map<String, ConnectorConfigChecker> map;

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        printParams(params);
        String name = params.get(ConfigConstant.CONFIG_MODEL_NAME);
        String connectorType = params.get("connectorType");
        Assert.hasText(name, "connector name is empty.");
        Assert.hasText(connectorType, "connector connectorType is empty.");

        Connector connector = new Connector();
        connector.setName(name);
        connector.setType(ConfigConstant.CONNECTOR);
        AbstractConnectorConfig config = getConfig(connectorType);
        connector.setConfig(config);

        // 配置连接器配置
        String type = StringUtil.toLowerCaseFirstOne(connectorType).concat("ConfigChecker");
        ConnectorConfigChecker checker = map.get(type);
        Assert.notNull(checker, "Checker can not be null.");
        checker.modify(config, params);

        // 获取表
        setTable(connector);

        // 修改基本配置
        this.modifyConfigModel(connector, params);

        return connector;
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) {
        printParams(params);
        Assert.notEmpty(params, "ConnectorChecker check params is null.");
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Connector connector = manager.getConnector(id);
        Assert.notNull(connector, "Can not find connector.");

        // 修改基本配置
        this.modifyConfigModel(connector, params);

        // 配置连接器配置
        AbstractConnectorConfig config = connector.getConfig();
        String type = StringUtil.toLowerCaseFirstOne(config.getConnectorType()).concat("ConfigChecker");
        ConnectorConfigChecker checker = map.get(type);
        Assert.notNull(checker, "Checker can not be null.");
        checker.modify(config, params);

        // 获取表
        setTable(connector);

        return connector;
    }

    private AbstractConnectorConfig getConfig(String connectorType) {
        Class<AbstractConnectorConfig> configClass = (Class<AbstractConnectorConfig>) ConnectorEnum.getConfigClass(connectorType);
        Assert.notNull(configClass, String.format("不支持该连接器类型:%s", connectorType));
        try {
            AbstractConnectorConfig config = configClass.newInstance();
            config.setConnectorType(connectorType);
            return config;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new BizException("获取连接器配置异常.");
        }
    }

    private void setTable(Connector connector) {
        boolean isAlive = manager.refreshConnectorConfig(connector.getConfig());
        if (!isAlive) {
            logService.log(LogType.ConnectorLog.FAILED);
        }
        Assert.isTrue(isAlive, "无法连接.");
        // 获取表信息
        ConnectorMapper connectorMapper = manager.connect(connector.getConfig());
        List<Table> table = manager.getTable(connectorMapper);
        connector.setTable(table);
    }

}