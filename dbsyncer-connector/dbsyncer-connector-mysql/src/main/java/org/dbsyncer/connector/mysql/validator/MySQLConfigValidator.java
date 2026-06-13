/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.validator;

import org.dbsyncer.connector.mysql.constant.MySQLConstant;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.AbstractDataBaseConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020-01-08 15:17
 */
public final class MySQLConfigValidator extends AbstractDataBaseConfigValidator {

    @Override
    public void modify(AbstractDatabaseConnector connectorService, DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorService, connectorConfig, params);
        MySQLConstant.enrichJdbcProperties(connectorConfig);
    }
}
