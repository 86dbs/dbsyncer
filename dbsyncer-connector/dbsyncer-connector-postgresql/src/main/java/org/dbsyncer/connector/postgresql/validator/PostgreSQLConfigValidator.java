/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.validator;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.AbstractDataBaseConfigValidator;

import java.util.Map;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-04-05 22:14
 */
public class PostgreSQLConfigValidator extends AbstractDataBaseConfigValidator {
    @Override
    public void modify(DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorConfig, params);
        super.modifySchema(connectorConfig, params);

        connectorConfig.getProperties().put("dropSlotOnClose", StringUtil.isNotBlank(params.get("dropSlotOnClose")) ? "true" : "false");
        connectorConfig.getProperties().put("pluginName", params.get("pluginName"));
    }
}
