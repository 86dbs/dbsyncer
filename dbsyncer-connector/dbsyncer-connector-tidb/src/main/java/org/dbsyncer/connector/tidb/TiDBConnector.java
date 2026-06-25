/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.tidb;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.mysql.MySQLConnector;
import org.dbsyncer.connector.tidb.constant.TiDBConstant;
import org.dbsyncer.connector.tidb.schema.TiDBSchemaResolver;
import org.dbsyncer.connector.tidb.validator.TiDBConfigValidator;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.schema.SchemaResolver;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TiDB 连接器（兼容 MySQL 协议）
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 00:10
 */
public final class TiDBConnector extends MySQLConnector {

    private static final Set<String> SYSTEM_DATABASES = Stream.of(
            "information_schema", "mysql", "performance_schema", "sys", "metrics_schema")
            .collect(Collectors.toSet());

    private final TiDBConfigValidator configValidator = new TiDBConfigValidator();
    private final TiDBSchemaResolver schemaResolver = new TiDBSchemaResolver();

    @Override
    public String getConnectorType() {
        return "TiDB";
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
    public ConnectorInstance connect(DatabaseConfig config, ConnectorServiceContext context) {
        TiDBConstant.enrichJdbcProperties(config);
        return super.connect(config, context);
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
