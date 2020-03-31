package org.dbsyncer.biz.checker.impl;

import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/7 23:16
 */
@Component
public class MysqlChecker extends AbstractChecker {

    @Override
    public void modify(Connector connector, Map<String, String> params) {
        String name = params.get(ConfigConstant.CONFIG_MODEL_NAME);
        String username = params.get("username");
        String password = params.get("password");
        String url = params.get("url");
        Assert.hasText(name, "MysqlChecker modify name is empty.");
        Assert.hasText(username, "MysqlChecker modify username is empty.");
        Assert.hasText(password, "MysqlChecker modify password is empty.");
        Assert.hasText(url, "MysqlChecker modify url is empty.");

        connector.setName(name);
        DatabaseConfig config = (DatabaseConfig) connector.getConfig();
        config.setUsername(username);
        config.setPassword(password);
        config.setUrl(url);
        connector.setUpdateTime(System.currentTimeMillis());
    }

}