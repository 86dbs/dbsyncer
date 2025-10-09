/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql;

import org.dbsyncer.connector.mysql.cdc.MySQLListener;
import org.dbsyncer.connector.mysql.schema.MySQLDateValueMapper;
import org.dbsyncer.connector.mysql.schema.MySQLSchemaResolver;
import org.dbsyncer.connector.mysql.storage.MySQLStorageService;
import org.dbsyncer.connector.mysql.validator.MySQLConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.sql.SqlTemplate;
import org.dbsyncer.sdk.connector.database.sql.context.SqlBuildContext;
import org.dbsyncer.sdk.connector.database.sql.impl.MySQLTemplate;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * MySQL连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-11-22 23:55
 */
public class MySQLConnector extends AbstractDatabaseConnector {


    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected final ConfigValidator<?> configValidator = new MySQLConfigValidator();
    private final MySQLSchemaResolver schemaResolver = new MySQLSchemaResolver();

    public MySQLConnector() {
        sqlTemplate = new MySQLTemplate();
        VALUE_MAPPERS.put(Types.DATE, new MySQLDateValueMapper());
    }

    @Override
    public String getConnectorType() {
        return "MySQL";
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new MySQLListener();
        }
        return null;
    }

    @Override
    public StorageService getStorageService() {
        return new MySQLStorageService();
    }

    @Override
    public String generateUniqueCode() {
        return DatabaseConstant.DBS_UNIQUE_CODE;
    }

    @Override
    public Map<String, String> getPosition(
            org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance connectorInstance) {
        // 执行SHOW MASTER STATUS命令获取当前binlog位置
        Map<String, Object> result = connectorInstance
                .execute(databaseTemplate -> databaseTemplate.queryForMap("SHOW MASTER STATUS"));

        if (result == null || result.isEmpty()) {
            throw new RuntimeException("获取MySQL当前binlog位置失败");
        }
        Map<String, String> position = new HashMap<>();
        position.put("fileName", (String) result.get("File"));
        position.put("position", String.valueOf(result.get("Position")));
        return position;
    }

    @Override
    public Integer getStreamingFetchSize(ReaderContext context) {
        return Integer.MIN_VALUE; // MySQL流式处理特殊值
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }



}