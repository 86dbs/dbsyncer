/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.sdk.config.DatabaseConfig;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-11-25 23:10
 */
@Component
public class SqlServerConfigChecker extends AbstractDataBaseConfigValidator {
    @Override
    public void modify(DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorConfig, params);
        super.modifySchema(connectorConfig, params);
    }
}