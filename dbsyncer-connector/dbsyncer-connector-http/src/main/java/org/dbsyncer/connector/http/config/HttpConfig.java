/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http.config;

import org.dbsyncer.connector.http.util.HttpUtil;
import org.dbsyncer.sdk.model.ConnectorConfig;

/**
 * Http连接配置
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-02 00:01
 */
public class HttpConfig extends ConnectorConfig {

    private String url;

    private boolean enableEncrypt;

    private boolean publicNetwork;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public boolean isEnableEncrypt() {
        return enableEncrypt;
    }

    public void setEnableEncrypt(boolean enableEncrypt) {
        this.enableEncrypt = enableEncrypt;
    }

    public boolean isPublicNetwork() {
        return publicNetwork;
    }

    public void setPublicNetwork(boolean publicNetwork) {
        this.publicNetwork = publicNetwork;
    }

    @Override
    public String getPropertiesText() {
        return HttpUtil.toString(getProperties());
    }
}