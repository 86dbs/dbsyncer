/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.tidb.validator;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.mysql.validator.MySQLConfigValidator;
import org.dbsyncer.connector.tidb.constant.TiDBConstant;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;

import java.util.Map;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 00:10
 */
public final class TiDBConfigValidator extends MySQLConfigValidator {

    @Override
    public void modify(AbstractDatabaseConnector connectorService, DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorService, connectorConfig, params);
        if (StringUtil.isBlank(connectorConfig.getDriverClassName())) {
            connectorConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
        }
        TiDBConstant.enrichJdbcProperties(connectorConfig);
    }
}
