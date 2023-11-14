package org.dbsyncer.sdk.model;

/**
 * 连接器配置
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/21 0:01
 */
public abstract class ConnectorConfig {

    /**
     * 连接器类型
     */
    private String connectorType;

    public String getConnectorType() {
        return connectorType;
    }

    public ConnectorConfig setConnectorType(String connectorType) {
        this.connectorType = connectorType;
        return this;
    }

}
