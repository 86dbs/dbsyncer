/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.model.SqlTable;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;

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
        Assert.hasText(username, "Username is empty.");
        Assert.hasText(password, "Password is empty.");
        Assert.hasText(url, "Url is empty.");

        connectorConfig.setUsername(username);
        connectorConfig.setPassword(password);
        connectorConfig.setUrl(url);
        connectorConfig.setDriverClassName(driverClassName);
    }

    protected void modifyDql(DatabaseConfig connectorConfig, Map<String, String> params) {
        String sqlTableParams = params.get("sqlTableParams");
        Assert.hasText(sqlTableParams, "sqlTableParams is empty.");
        List<SqlTable> sqlTables = JsonUtil.jsonToArray(sqlTableParams, SqlTable.class);
        Assert.isTrue(!CollectionUtils.isEmpty(sqlTables), "sqlTables is empty.");
        sqlTables.forEach(sqlTable -> {
            Assert.hasText(sqlTable.getSqlName(), "SqlName is empty.");
            Assert.hasText(sqlTable.getSql(), "Sql is empty.");
            Assert.hasText(sqlTable.getTable(), "Table is empty.");
        });
        connectorConfig.setSqlTables(sqlTables);
    }

    protected void modifySchema(DatabaseConfig connectorConfig, Map<String, String> params) {
        String schema = params.get("schema");
        Assert.hasText(schema, "Schema is empty.");
        connectorConfig.setSchema(schema);
    }

}