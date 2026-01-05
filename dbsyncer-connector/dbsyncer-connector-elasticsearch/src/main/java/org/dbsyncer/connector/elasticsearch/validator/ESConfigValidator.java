/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.validator;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.connector.elasticsearch.ElasticsearchConnector;
import org.dbsyncer.connector.elasticsearch.config.ESConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.model.Table;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * ES连接配置校验器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-08-25 23:30
 */
public final class ESConfigValidator implements ConfigValidator<ElasticsearchConnector, ESConfig> {

    @Override
    public void modify(ElasticsearchConnector connectorService, ESConfig connectorConfig, Map<String, String> params) {
        String username = params.get("username");
        String password = params.get("password");
        String url = params.get("url");
        String timeoutSeconds = params.get("timeoutSeconds");
        Assert.hasText(username, "Username is empty.");
        Assert.hasText(password, "Password is empty.");
        Assert.hasText(url, "Url is empty.");

        connectorConfig.setUsername(username);
        connectorConfig.setPassword(password);
        connectorConfig.setUrl(url);
        connectorConfig.setTimeoutSeconds(NumberUtil.toInt(timeoutSeconds));
    }

    @Override
    public Table modifyExtendedTable(ElasticsearchConnector connectorService, Map<String, String> params) {
        return null;
    }
}