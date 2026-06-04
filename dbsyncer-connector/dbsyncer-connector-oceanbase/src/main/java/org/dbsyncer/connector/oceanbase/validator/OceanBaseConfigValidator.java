/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbase.validator;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oceanbase.cdc.OceanBaseCdcConstants;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.AbstractDataBaseConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;

import org.springframework.util.Assert;

import java.util.Map;

/**
 * OceanBase 连接配置校验
 */
public class OceanBaseConfigValidator extends AbstractDataBaseConfigValidator {

    private static final String DEFAULT_DRIVER = "com.oceanbase.jdbc.Driver";

    @Override
    public void modify(AbstractDatabaseConnector connectorService, DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorService, connectorConfig, params);
        if (StringUtil.isBlank(connectorConfig.getDriverClassName())) {
            connectorConfig.setDriverClassName(DEFAULT_DRIVER);
        }
        putExtInfo(connectorConfig, OceanBaseCdcConstants.LOG_PROXY_HOST, params.get("logProxyHost"));
        putExtInfo(connectorConfig, OceanBaseCdcConstants.LOG_PROXY_PORT, params.get("logProxyPort"));
        putExtInfo(connectorConfig, OceanBaseCdcConstants.TENANT_NAME, params.get("tenantName"));
        putExtInfo(connectorConfig, OceanBaseCdcConstants.RS_LIST, params.get("rsList"));
        putExtInfo(connectorConfig, OceanBaseCdcConstants.CONFIG_URL, params.get("configUrl"));
        putExtInfo(connectorConfig, OceanBaseCdcConstants.WORKING_MODE, params.get("workingMode"));
        putExtInfo(connectorConfig, OceanBaseCdcConstants.TIMEZONE, params.get("timezone"));
        putExtInfo(connectorConfig, OceanBaseCdcConstants.SYS_USERNAME, params.get("sysUsername"));
        putExtInfo(connectorConfig, OceanBaseCdcConstants.SYS_PASSWORD, params.get("sysPassword"));

        String logProxyHost = connectorConfig.getExtInfo().getProperty(OceanBaseCdcConstants.LOG_PROXY_HOST);
        Assert.hasText(logProxyHost, "LogProxy 主机不能为空");
        int logProxyPort = NumberUtil.toInt(connectorConfig.getExtInfo().getProperty(OceanBaseCdcConstants.LOG_PROXY_PORT), 2983);
        Assert.isTrue(logProxyPort > 0, "LogProxy 端口无效");
        connectorConfig.getExtInfo().setProperty(OceanBaseCdcConstants.LOG_PROXY_PORT, String.valueOf(logProxyPort));
        Assert.hasText(connectorConfig.getExtInfo().getProperty(OceanBaseCdcConstants.TENANT_NAME), "租户名称不能为空");
    }

    private void putExtInfo(DatabaseConfig config, String key, String value) {
        if (StringUtil.isNotBlank(value)) {
            config.getExtInfo().setProperty(key, value.trim());
        }
    }
}
