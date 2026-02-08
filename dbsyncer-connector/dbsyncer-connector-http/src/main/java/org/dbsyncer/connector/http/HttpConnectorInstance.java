
/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http;

import org.dbsyncer.connector.http.config.HttpConfig;
import org.dbsyncer.sdk.connector.ConnectorInstance;

/**
 * Http连接器实例
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-02 00:01
 */
public final class HttpConnectorInstance implements ConnectorInstance<HttpConfig, Object> {

    private HttpConfig config;

    public HttpConnectorInstance(HttpConfig config) {
        this.config = config;
    }

    @Override
    public String getServiceUrl() {
        return config.getUrl();
    }

    @Override
    public HttpConfig getConfig() {
        return config;
    }

    @Override
    public void setConfig(HttpConfig config) {
        this.config = config;
    }

    @Override
    public Object getConnection() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

}