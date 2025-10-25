/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.validator;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.AbstractDataBaseConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.util.DatabaseUtil;

import java.util.Map;
import java.util.Properties;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-04-05 22:14
 */
public class PostgreSQLConfigValidator extends AbstractDataBaseConfigValidator {
    @Override
    public void modify(AbstractDatabaseConnector connectorService, DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorService, connectorConfig, params);

        Properties properties = DatabaseUtil.parseJdbcProperties(connectorConfig.getProperties());
        properties.put("dropSlotOnClose", StringUtil.isNotBlank(params.get("dropSlotOnClose")) ? "true" : "false");
        properties.put("pluginName", params.get("pluginName"));
        connectorConfig.setProperties(DatabaseUtil.propertiesToParams(properties));
    }
}
