/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.DatabaseConfig;

import java.util.Map;

/**
 * Oracle连接配置校验器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-08 15:17
 */
public class OracleConfigValidator extends AbstractDataBaseConfigValidator {

    @Override
    public void modify(DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorConfig, params);

        String schema = params.get("schema");
        connectorConfig.setSchema(StringUtil.isBlank(schema) ? connectorConfig.getUsername().toUpperCase() : schema.toUpperCase());
    }

}