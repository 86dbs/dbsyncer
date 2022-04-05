package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.PostgreSQLConfig;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/5 22:14
 */
@Component
public class PostgreSQLConfigChecker extends AbstractDataBaseConfigChecker {
    @Override
    public void modify(DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorConfig, params);
        String schema = params.get("schema");
        Assert.hasText(schema, "Schema is empty.");

        PostgreSQLConfig config = (PostgreSQLConfig) connectorConfig;
        config.setSchema(schema);
    }
}
