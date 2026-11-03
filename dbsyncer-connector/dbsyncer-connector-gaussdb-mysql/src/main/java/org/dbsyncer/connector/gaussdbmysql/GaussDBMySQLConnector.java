/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.gaussdbmysql;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.gaussdbmysql.schema.GaussDBMySQLSchemaResolver;
import org.dbsyncer.connector.gaussdbmysql.validator.GaussDBMySQLConfigValidator;
import org.dbsyncer.connector.mysql.MySQLConnector;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.schema.SchemaResolver;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * GaussDB MySQL 兼容连接器（适用于 GaussDB for MySQL）
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-29 14:50
 */
public final class GaussDBMySQLConnector extends MySQLConnector {

    private static final Set<String> SYSTEM_DATABASES = Stream.of(
                    "information_schema", "mysql", "performance_schema", "sys")
            .collect(Collectors.toSet());

    private final GaussDBMySQLConfigValidator configValidator = new GaussDBMySQLConfigValidator();
    private final GaussDBMySQLSchemaResolver schemaResolver = new GaussDBMySQLSchemaResolver();

    @Override
    public String getConnectorType() {
        return "GaussDBMySQL";
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    @Override
    public List<String> getDatabases(DatabaseConnectorInstance connectorInstance) {
        return connectorInstance.execute(databaseTemplate -> {
            List<String> databases = databaseTemplate.queryForList("SHOW DATABASES", String.class);
            if (CollectionUtils.isEmpty(databases)) {
                return Collections.emptyList();
            }
            return databases.stream()
                    .filter(name -> !SYSTEM_DATABASES.contains(name.toLowerCase()))
                    .collect(Collectors.toList());
        });
    }
}
