/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.kingbase.validator;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.kingbase.constant.KingbaseConfigConstant;
import org.dbsyncer.connector.postgresql.validator.PostgreSQLConfigValidator;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;

import java.util.Map;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 18:30
 */
public final class KingbaseConfigValidator extends PostgreSQLConfigValidator {

    @Override
    public void modify(AbstractDatabaseConnector connectorService, DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorService, connectorConfig, params);
        if (StringUtil.isBlank(connectorConfig.getDriverClassName())) {
            connectorConfig.setDriverClassName("com.kingbase8.Driver");
        }
        String pluginName = params.get(KingbaseConfigConstant.PLUGIN_NAME);
        if (StringUtil.isBlank(pluginName) || "pgoutput".equalsIgnoreCase(pluginName)) {
            connectorConfig.getExtInfo().put(KingbaseConfigConstant.PLUGIN_NAME, KingbaseConfigConstant.DEFAULT_PLUGIN);
        }
    }
}