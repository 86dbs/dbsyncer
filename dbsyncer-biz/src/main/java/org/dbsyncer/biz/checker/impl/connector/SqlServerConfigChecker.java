package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.sdk.config.DatabaseConfig;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/5/18 20:17
 */
@Component
public class SqlServerConfigChecker extends AbstractDataBaseConfigChecker {
    @Override
    public void modify(DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorConfig, params);
        super.modifySchema(connectorConfig, params);
    }
}