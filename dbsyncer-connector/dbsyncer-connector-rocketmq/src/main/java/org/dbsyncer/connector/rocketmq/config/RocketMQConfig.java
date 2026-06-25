/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.rocketmq.config;

import org.dbsyncer.connector.rocketmq.util.RocketMQUtil;
import org.dbsyncer.sdk.model.ConnectorConfig;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 01:00
 */
public class RocketMQConfig extends ConnectorConfig {

    private String url;

    @Override
    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String getPropertiesText() {
        return RocketMQUtil.toString(getProperties());
    }
}
