/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.gaussdb.validator;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.gaussdb.constant.GaussDBConfigConstant;
import org.dbsyncer.connector.postgresql.validator.PostgreSQLConfigValidator;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;

import java.util.Map;

/**
 * GaussDB 配置校验
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-29 14:50
 */
public final class GaussDBConfigValidator extends PostgreSQLConfigValidator {

    @Override
    public void modify(AbstractDatabaseConnector connectorService, DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorService, connectorConfig, params);
        if (StringUtil.isBlank(connectorConfig.getDriverClassName())) {
            connectorConfig.setDriverClassName(GaussDBConfigConstant.DEFAULT_DRIVER);
        }
        String pluginName = params.get(GaussDBConfigConstant.PLUGIN_NAME);
        if (StringUtil.isBlank(pluginName) || "pgoutput".equalsIgnoreCase(pluginName)) {
            connectorConfig.getExtInfo().put(GaussDBConfigConstant.PLUGIN_NAME, GaussDBConfigConstant.DEFAULT_PLUGIN);
        }
    }
}
