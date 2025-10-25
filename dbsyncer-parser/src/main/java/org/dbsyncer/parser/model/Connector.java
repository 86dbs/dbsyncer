package org.dbsyncer.parser.model;

import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.ConnectorConfig;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/1 12:40
 */
public class Connector extends ConfigModel{

    public Connector() {
        super.setType(ConfigConstant.CONNECTOR);
    }

    /**
     * 连接器配置
     */
    private ConnectorConfig config;

    public ConnectorConfig getConfig() {
        return config;
    }

    public Connector setConfig(ConnectorConfig config) {
        this.config = config;
        return this;
    }
}
