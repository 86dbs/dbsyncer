package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.biz.checker.ConnectorConfigChecker;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.parser.model.Connector;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class DqlMysqlConfigChecker implements ConnectorConfigChecker {

    @Override
    public void modify(Connector connector, Map<String, String> params) {
        String username = params.get("username");
        String password = params.get("password");
        String url = params.get("url");
        String sql = params.get("sql");
        Assert.hasText(username, "DqlMysqlConfigChecker modify username is empty.");
        Assert.hasText(password, "DqlMysqlConfigChecker modify password is empty.");
        Assert.hasText(url, "DqlMysqlConfigChecker modify url is empty.");
        Assert.hasText(sql, "DqlMysqlConfigChecker modify sql is empty.");

        DatabaseConfig config = (DatabaseConfig) connector.getConfig();
        config.setUsername(username);
        config.setPassword(password);
        config.setUrl(url);
        config.setSql(sql);
    }
}