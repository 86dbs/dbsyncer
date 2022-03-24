package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.SqlServerDatabaseConfig;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/5/18 20:17
 */
@Component
public class DqlSqlServerConfigChecker extends AbstractDataBaseConfigChecker {

    @Override
    public void modify(DatabaseConfig connectorConfig, Map<String, String> params) {
        super.modify(connectorConfig, params);
        super.modifyDql(connectorConfig, params);
        String schema = params.get("schema");
        Assert.hasText(schema, "Schema is empty.");

        SqlServerDatabaseConfig config = (SqlServerDatabaseConfig) connectorConfig;
        config.setSchema(schema);
    }
}