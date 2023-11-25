/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-08 15:17
 */
@Component
public class DqlOracleConfigChecker extends AbstractDataBaseConfigValidator {

    @Override
    public void modify(DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorConfig, params);
        super.modifyDql(connectorConfig, params);

        String schema = params.get("schema");
        connectorConfig.setSchema(StringUtil.isBlank(schema) ? connectorConfig.getUsername().toUpperCase() : schema.toUpperCase());
    }
}