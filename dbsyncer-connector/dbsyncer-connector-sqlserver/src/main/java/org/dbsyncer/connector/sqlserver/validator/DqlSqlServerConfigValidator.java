/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.validator;

import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.AbstractDataBaseConfigValidator;

import java.util.Map;

/**
 * DqlSqlServer连接配置校验器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-18 20:17
 */
public class DqlSqlServerConfigValidator extends AbstractDataBaseConfigValidator {

    @Override
    public void modify(DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorConfig, params);
        super.modifyDql(connectorConfig, params);
        super.modifySchema(connectorConfig, params);
    }
}