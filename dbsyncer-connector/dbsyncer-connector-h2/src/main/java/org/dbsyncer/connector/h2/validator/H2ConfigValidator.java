/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.h2.validator;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.AbstractDataBaseConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.util.PropertiesUtil;

import org.springframework.util.Assert;

import java.util.Map;
import java.util.Objects;

/**
 * H2连接配置校验器
 */
public final class H2ConfigValidator extends AbstractDataBaseConfigValidator {

    @Override
    public void modify(AbstractDatabaseConnector connectorService, DatabaseConfig connectorConfig, Map<String, String> params) {
        String properties = params.get("properties");
        String serviceName = params.get("serviceName");
        String username = params.get("username");
        String password = params.get("password");
        String driverClassName = params.get("driverClassName");
        int maxActive = NumberUtil.toInt(Objects.toString(params.get("maxActive")), connectorConfig.getMaxActive());
        long keepAlive = NumberUtil.toLong(Objects.toString(params.get("keepAlive")), connectorConfig.getKeepAlive());
        Assert.hasText(serviceName, "数据库文件路径不能为空.");
        Assert.isTrue(maxActive >= 1 && maxActive <= 512, "最大连接数只允许输入1-512.");
        Assert.isTrue(keepAlive >= 10000 && keepAlive <= 120000, "有效期只允许输入10000-120000.");

        connectorConfig.setServiceName(serviceName);
        connectorConfig.setUsername(StringUtil.isNotBlank(username) ? username.trim() : "sa");
        connectorConfig.setPassword(password != null ? password : StringUtil.EMPTY);
        connectorConfig.setProperties(PropertiesUtil.parse(properties));
        connectorConfig.setUrl(connectorService.buildJdbcUrl(connectorConfig, serviceName));
        connectorConfig.setDriverClassName(StringUtil.isNotBlank(driverClassName) ? driverClassName : "org.h2.Driver");
        connectorConfig.setMaxActive(maxActive);
        connectorConfig.setKeepAlive(keepAlive);
    }
}
