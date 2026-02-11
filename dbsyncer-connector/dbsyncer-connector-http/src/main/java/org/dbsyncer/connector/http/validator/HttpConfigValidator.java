/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http.validator;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.http.HttpConnector;
import org.dbsyncer.connector.http.config.HttpConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;

import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;

/**
 * Http连接配置校验器实现
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-02 00:01
 */
@Component
public class HttpConfigValidator implements ConfigValidator<HttpConnector, HttpConfig> {

    @Override
    public void modify(HttpConnector connectorService, HttpConfig connectorConfig, Map<String, String> params) {
        String url = params.get("url");
        String properties = params.get("properties");
        Assert.hasText(url, "url is empty.");
        Assert.hasText(properties, "properties is empty.");
        connectorConfig.setUrl(url);
    }

    @Override
    public Table modifyExtendedTable(HttpConnector connectorService, Map<String, String> params) {
        Table table = new Table();
        String tableName = params.get("tableName");
        String columnList = params.get("columnList");
        String method = params.get("method");
        String api = params.get("api");
        String contentType = params.get("contentType");
        String requestParams = params.get("params");
        String extractData = params.get("extractData");
        String extractTotal = params.get("extractTotal");
        Assert.hasText(tableName, "TableName is empty");
        Assert.hasText(columnList, "ColumnList is empty");
        Assert.hasText(method, "请求方式 is empty");
        Assert.hasText(api, "接口不能为空");
        Assert.hasText(contentType, "ContentType不能为空");
        Assert.hasText(requestParams, "动态参数不能为空");
        Assert.hasText(extractData, "解析数据规则不能为空");
        Assert.hasText(extractTotal, "解析总数规则不能为空");
        List<Field> fields = JsonUtil.jsonToArray(columnList, Field.class);
        Assert.notEmpty(fields, "字段不能为空.");
        table.setName(tableName);
        table.setColumn(fields);
        table.setType(connectorService.getExtendedTableType().getCode());
        table.getExtInfo().put("method", method);
        table.getExtInfo().put("api", api);
        table.getExtInfo().put("contentType", contentType);
        table.getExtInfo().put("params", requestParams);
        table.getExtInfo().put("extractData", extractData);
        table.getExtInfo().put("extractTotal", extractTotal);
        return table;
    }

}