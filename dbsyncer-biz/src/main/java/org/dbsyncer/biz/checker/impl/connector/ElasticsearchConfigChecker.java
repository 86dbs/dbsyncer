package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.biz.checker.ConnectorConfigChecker;
import org.dbsyncer.connector.config.ESConfig;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/8/25 23:30
 */
@Component
public class ElasticsearchConfigChecker implements ConnectorConfigChecker<ESConfig> {

    @Override
    public void modify(ESConfig connectorConfig, Map<String, String> params) {
        String username = params.get("username");
        String password = params.get("password");
        String index = params.get("index");
        String url = params.get("url");
        Assert.hasText(username, "Username is empty.");
        Assert.hasText(password, "Password is empty.");
        Assert.hasText(index, "Index is empty.");
        Assert.hasText(url, "Url is empty.");

        connectorConfig.setUsername(username);
        connectorConfig.setPassword(password);
        connectorConfig.setIndex(index);
        connectorConfig.setUrl(url);
    }
}