/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.model.SqlTable;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 关系型数据库连接配置校验器
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-08 15:17
 */
public abstract class AbstractDataBaseConfigValidator implements ConfigValidator<DatabaseConfig> {

    @Override
    public void modify(DatabaseConfig connectorConfig, Map<String, String> params) {
        String username = params.get("username");
        String password = params.get("password");
        String url = params.get("url");
        String driverClassName = params.get("driverClassName");
        int maxActive = NumberUtil.toInt(Objects.toString(params.get("maxActive")), connectorConfig.getMaxActive());
        long keepAlive = NumberUtil.toLong(Objects.toString(params.get("keepAlive")), connectorConfig.getKeepAlive());
        Assert.hasText(username, "Username is empty.");
        Assert.hasText(password, "Password is empty.");
        Assert.hasText(url, "Url is empty.");
        Assert.isTrue(maxActive >= 1 && maxActive <= 512, "最大连接数只允许输入1-512.");
        Assert.isTrue(keepAlive >= 10000 && keepAlive <= 120000, "有效期只允许输入10000-120000.");

        connectorConfig.setUsername(username);
        connectorConfig.setPassword(password);
        connectorConfig.setUrl(url);
        connectorConfig.setDriverClassName(driverClassName);
        connectorConfig.setMaxActive(maxActive);
        connectorConfig.setKeepAlive(keepAlive);
    }

    protected void modifyDql(DatabaseConfig connectorConfig, Map<String, String> params) {
        Object sqlTableParams = params.get("sqlTables");
        Assert.notNull(sqlTableParams, "sqlTables is null.");
        String sqlTables = (sqlTableParams instanceof String) ? (String) sqlTableParams : JsonUtil.objToJson(sqlTableParams);
        Assert.hasText(sqlTables, "sqlTables is empty.");
        List<SqlTable> sqlTableArray = JsonUtil.jsonToArray(sqlTables, SqlTable.class);
        Assert.isTrue(!CollectionUtils.isEmpty(sqlTableArray), "sqlTableArray is empty.");
        sqlTableArray.forEach(sqlTable -> {
            Assert.hasText(sqlTable.getSqlName(), "SqlName is empty.");
            Assert.hasText(sqlTable.getSql(), "Sql is empty.");
            Assert.hasText(sqlTable.getTable(), "Table is empty.");
        });
        connectorConfig.setSqlTables(sqlTableArray);
    }

    protected void modifySchema(DatabaseConfig connectorConfig, Map<String, String> params) {
        String schema = params.get("schema");
        Assert.hasText(schema, "Schema is empty.");
        connectorConfig.setSchema(schema);
    }

}