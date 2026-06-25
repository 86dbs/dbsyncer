/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.starrocks;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.mysql.MySQLConnector;
import org.dbsyncer.connector.starrocks.constant.StarRocksConstant;
import org.dbsyncer.connector.starrocks.load.StarRocksStreamLoadWriter;
import org.dbsyncer.connector.starrocks.schema.StarRocksSchemaResolver;
import org.dbsyncer.connector.starrocks.validator.StarRocksConfigValidator;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.storage.StorageService;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * StarRocks 连接器（兼容 MySQL 协议）
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 03:00
 */
public final class StarRocksConnector extends MySQLConnector {

    private static final Set<String> SYSTEM_DATABASES = Stream.of(
            "information_schema", "sys", "_statistics_")
            .collect(Collectors.toSet());

    private final StarRocksConfigValidator configValidator = new StarRocksConfigValidator();
    private final StarRocksSchemaResolver schemaResolver = new StarRocksSchemaResolver();
    private final StarRocksStreamLoadWriter streamLoadWriter = new StarRocksStreamLoadWriter();

    @Override
    public String getConnectorType() {
        return "StarRocks";
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
        StarRocksConstant.enrichJdbcProperties(config);
        return super.connect(config, context);
    }

    @Override
    public Result writer(DatabaseConnectorInstance connectorInstance, PluginContext context) {
        DatabaseConfig config = connectorInstance.getConfig();
        if (StarRocksConstant.isStreamLoadMode(config) && StarRocksConstant.isStreamLoadEvent(context.getEvent())) {
            return streamLoadWriter.write(connectorInstance, context);
        }
        return super.writer(connectorInstance, context);
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }
        return null;
    }

    @Override
    public StorageService getStorageService() {
        return null;
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
