package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.biz.checker.ConnectorConfigChecker;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.spi.ConnectorMapper;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
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

    @Resource
    private ParserComponent parserComponent;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private LogService logService;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
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
        ConnectorConfig config = getConfig(connectorType);
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
        Connector connector = profileComponent.getConnector(id);
        Assert.notNull(connector, "Can not find connector.");

        // 修改基本配置
        this.modifyConfigModel(connector, params);

        // 配置连接器配置
        ConnectorConfig config = connector.getConfig();
        String type = StringUtil.toLowerCaseFirstOne(config.getConnectorType()).concat("ConfigChecker");
        ConnectorConfigChecker checker = map.get(type);
        Assert.notNull(checker, "Checker can not be null.");
        checker.modify(config, params);

        // 获取表
        setTable(connector);

        return connector;
    }

    private ConnectorConfig getConfig(String connectorType) {
        try {
            ConnectorConfig config = ConnectorEnum.getConnectorEnum(connectorType).getConfigClass().newInstance();
            config.setConnectorType(connectorType);
            return config;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new BizException("获取连接器配置异常.");
        }
    }

    private void setTable(Connector connector) {
        boolean isAlive = connectorFactory.isAlive(connector.getConfig());
        if (!isAlive) {
            logService.log(LogType.ConnectorLog.FAILED);
        }
        Assert.isTrue(isAlive, "无法连接.");
        // 获取表信息
        ConnectorMapper connectorMapper = connectorFactory.connect(connector.getConfig());
        List<Table> table = parserComponent.getTable(connectorMapper);
        connector.setTable(table);
    }

}