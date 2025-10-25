/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.Objects;

/**
 * 关系型数据库连接配置校验器
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-08 15:17
 */
public abstract class AbstractDataBaseConfigValidator implements ConfigValidator<AbstractDatabaseConnector, DatabaseConfig> {

    @Override
    public void modify(AbstractDatabaseConnector connectorService, DatabaseConfig connectorConfig, Map<String, String> params) {
        String host = params.get("host");
        int port = NumberUtil.toInt(Objects.toString(params.get("port")));
        String username = params.get("username");
        String password = params.get("password");
        String properties = params.get("properties");
        String driverClassName = params.get("driverClassName");
        int maxActive = NumberUtil.toInt(Objects.toString(params.get("maxActive")), connectorConfig.getMaxActive());
        long keepAlive = NumberUtil.toLong(Objects.toString(params.get("keepAlive")), connectorConfig.getKeepAlive());
        Assert.hasText(host, "Host is empty.");
        Assert.isTrue(port > 0, "Port is invalid.");
        Assert.hasText(password, "Password is empty.");
        Assert.hasText(username, "Username is empty.");
        Assert.hasText(password, "Password is empty.");
        Assert.isTrue(maxActive >= 1 && maxActive <= 512, "最大连接数只允许输入1-512.");
        Assert.isTrue(keepAlive >= 10000 && keepAlive <= 120000, "有效期只允许输入10000-120000.");

        connectorConfig.setHost(host);
        connectorConfig.setPort(port);
        connectorConfig.setUsername(username);
        connectorConfig.setPassword(password);
        connectorConfig.setProperties(properties);
        connectorConfig.setUrl(connectorService.buildJdbcUrl(connectorConfig, StringUtil.EMPTY));
        connectorConfig.setDriverClassName(driverClassName);
        connectorConfig.setMaxActive(maxActive);
        connectorConfig.setKeepAlive(keepAlive);
    }

}