/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.validator;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.postgresql.constant.PostgreSQLConfigConstant;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.AbstractDataBaseConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;

import java.util.Map;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-04-05 22:14
 */
public class PostgreSQLConfigValidator extends AbstractDataBaseConfigValidator {

    @Override
    public void modify(AbstractDatabaseConnector connectorService, DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorService, connectorConfig, params);

        connectorConfig.getExtInfo().put(PostgreSQLConfigConstant.DROP_SLOT_ON_CLOSE, StringUtil.isNotBlank(params.get(PostgreSQLConfigConstant.DROP_SLOT_ON_CLOSE)) ? "true" : "false");
        connectorConfig.getExtInfo().put(PostgreSQLConfigConstant.PLUGIN_NAME, params.get("pluginName"));
    }
}
