/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.duckdb.validator;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.AbstractDataBaseConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.util.PropertiesUtil;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.Objects;

/**
 * DuckDB 连接配置校验器（文件库，serviceName 为数据库文件路径）
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-16 10:00
 */
public final class DuckDBConfigValidator extends AbstractDataBaseConfigValidator {

    @Override
    public void modify(AbstractDatabaseConnector connectorService, DatabaseConfig connectorConfig, Map<String, String> params) {
        String properties = params.get("properties");
        String serviceName = params.get("serviceName");
        String driverClassName = params.get("driverClassName");
        int maxActive = NumberUtil.toInt(Objects.toString(params.get("maxActive")), connectorConfig.getMaxActive());
        long keepAlive = NumberUtil.toLong(Objects.toString(params.get("keepAlive")), connectorConfig.getKeepAlive());
        Assert.hasText(serviceName, "数据库文件路径不能为空.");
        // DuckDB 单写多读，连接数不宜过大
        Assert.isTrue(maxActive >= 1 && maxActive <= 64, "最大连接数只允许输入1-64.");
        Assert.isTrue(keepAlive >= 10000 && keepAlive <= 120000, "有效期只允许输入10000-120000.");

        connectorConfig.setServiceName(serviceName);
        connectorConfig.setProperties(PropertiesUtil.parse(properties));
        connectorConfig.setUrl(connectorService.buildJdbcUrl(connectorConfig, serviceName));
        connectorConfig.setDriverClassName(driverClassName);
        connectorConfig.setMaxActive(maxActive);
        connectorConfig.setKeepAlive(keepAlive);
    }
}
