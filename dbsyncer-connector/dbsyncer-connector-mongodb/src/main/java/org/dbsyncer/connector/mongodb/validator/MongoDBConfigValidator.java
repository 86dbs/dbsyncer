/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.mongodb.validator;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.mongodb.MongoDBConnector;
import org.dbsyncer.connector.mongodb.config.MongoDBConfig;
import org.dbsyncer.connector.mongodb.util.MongoUtil;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.util.PropertiesUtil;

import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 20:00
 */
public final class MongoDBConfigValidator implements ConfigValidator<MongoDBConnector, MongoDBConfig> {

    @Override
    public void modify(MongoDBConnector connectorService, MongoDBConfig connectorConfig, Map<String, String> params) {
        String host = params.get("host");
        int port = NumberUtil.toInt(Objects.toString(params.get("port")));
        String username = params.get("username");
        String password = params.get("password");
        String database = params.get("database");
        String authDatabase = params.get("authDatabase");
        String properties = params.get("properties");
        int maxPoolSize = NumberUtil.toInt(Objects.toString(params.get("maxPoolSize")), connectorConfig.getMaxPoolSize());
        Assert.hasText(host, "Host is empty.");
        Assert.isTrue(port > 0, "Port is invalid.");
        Assert.hasText(database, "Database is empty.");
        Assert.isTrue(maxPoolSize >= 1 && maxPoolSize <= 512, "最大连接数只允许输入1-512.");

        connectorConfig.setHost(host);
        connectorConfig.setPort(port);
        connectorConfig.setUsername(username);
        connectorConfig.setPassword(password);
        connectorConfig.setDatabase(database);
        connectorConfig.setAuthDatabase(StringUtil.isNotBlank(authDatabase) ? authDatabase : "admin");
        connectorConfig.setMaxPoolSize(maxPoolSize);
        connectorConfig.setProperties(PropertiesUtil.parse(properties));
        connectorConfig.setUrl(MongoUtil.buildConnectionString(connectorConfig));
    }

    @Override
    public Table modifyExtendedTable(MongoDBConnector connectorService, Map<String, String> params) {
        Table table = new Table();
        String tableName = params.get("tableName");
        String columnList = params.get("columnList");
        Assert.hasText(tableName, "TableName is empty");
        Assert.hasText(columnList, "ColumnList is empty");
        List<Field> fields = JsonUtil.jsonToArray(columnList, Field.class);
        Assert.notEmpty(fields, "字段不能为空.");
        table.setName(tableName);
        table.setColumn(fields);
        table.setType(connectorService.getExtendedTableType().getCode());
        return table;
    }
}
