/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.validator;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.connector.elasticsearch.ElasticsearchConnector;
import org.dbsyncer.connector.elasticsearch.config.ESConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;

import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * ES连接配置校验器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-08-25 23:30
 */
public final class ESConfigValidator implements ConfigValidator<ElasticsearchConnector, ESConfig> {

    @Override
    public void modify(ElasticsearchConnector connectorService, ESConfig connectorConfig, Map<String, String> params) {
        String username = params.get("username");
        String password = params.get("password");
        String url = params.get("url");
        String timeoutSeconds = Objects.toString(params.get("timeoutSeconds"));
        Assert.hasText(url, "Url is empty.");

        connectorConfig.setUsername(username);
        connectorConfig.setPassword(password);
        connectorConfig.setUrl(url);
        connectorConfig.setTimeoutSeconds(NumberUtil.toInt(timeoutSeconds));
    }

    @Override
    public Table modifyExtendedTable(ElasticsearchConnector connectorService, Map<String, String> params) {
        Table table = new Table();
        String tableName = params.get("tableName");
        String columnList = params.get("columnList");
        String type = params.get(ElasticsearchConnector._TYPE);
        Assert.hasText(tableName, "TableName is empty");
        Assert.hasText(columnList, "ColumnList is empty");
        List<Field> fields = JsonUtil.jsonToArray(columnList, Field.class);
        Assert.notEmpty(fields, "字段不能为空.");
        table.setName(tableName);
        table.setColumn(fields);
        table.getExtInfo().put(ElasticsearchConnector._TYPE, type);
        table.setType(connectorService.getExtendedTableType().getCode());
        return table;
    }
}
