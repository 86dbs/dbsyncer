package org.dbsyncer.sdk.model;

import org.dbsyncer.sdk.util.PropertiesUtil;

import java.util.Properties;

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

    /**
     * 连接参数
     */
    private Properties properties = new Properties();

    /**
     * 扩展配置
     */
    private Properties extInfo = new Properties();

    public String getConnectorType() {
        return connectorType;
    }

    public ConnectorConfig setConnectorType(String connectorType) {
        this.connectorType = connectorType;
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public String getPropertiesText() {
        return PropertiesUtil.toString(properties);
    }

    public Properties getExtInfo() {
        return extInfo;
    }

    public void setExtInfo(Properties extInfo) {
        this.extInfo = extInfo;
    }
}
