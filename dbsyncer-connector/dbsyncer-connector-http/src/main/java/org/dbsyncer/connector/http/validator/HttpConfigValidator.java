/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http.validator;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.http.HttpConnector;
import org.dbsyncer.connector.http.config.HttpConfig;
import org.dbsyncer.connector.http.constant.HttpConstant;
import org.dbsyncer.connector.http.util.HttpUtil;
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
        connectorConfig.setEnableEncrypt(StringUtil.isNotBlank(params.get("enableEncrypt")));
        connectorConfig.setProperties(HttpUtil.parse(properties));
    }

    @Override
    public Table modifyExtendedTable(HttpConnector connectorService, Map<String, String> params) {
        Table table = new Table();
        String type = params.get("type");
        if ("source".equals(type)) {
            String extractPath = params.get(HttpConstant.EXTRACT_PATH);
            String extractTotal = params.get(HttpConstant.EXTRACT_TOTAL);
            Assert.hasText(extractPath, "解析数据规则不能为空");
            Assert.hasText(extractTotal, "解析总数规则不能为空");
            table.getExtInfo().put(HttpConstant.EXTRACT_PATH, extractPath);
            table.getExtInfo().put(HttpConstant.EXTRACT_TOTAL, extractTotal);
        } else {
            String writePath = params.get(HttpConstant.WRITE_PATH);
            if (StringUtil.isNotBlank(writePath)) {
                table.getExtInfo().put(HttpConstant.WRITE_PATH, writePath);
            }
        }

        String tableName = params.get("tableName");
        String columnList = params.get("columnList");
        String method = params.get(HttpConstant.METHOD);
        String api = params.get(HttpConstant.API);
        String contentType = params.get(HttpConstant.CONTENT_TYPE);
        String requestParams = params.get(HttpConstant.PARAMS);
        Assert.hasText(tableName, "TableName is empty");
        Assert.hasText(columnList, "ColumnList is empty");
        Assert.hasText(method, "请求方式 is empty");
        Assert.hasText(api, "接口不能为空");
        Assert.hasText(contentType, "ContentType不能为空");
        Assert.hasText(requestParams, "请求参数不能为空");
        List<Field> fields = JsonUtil.jsonToArray(columnList, Field.class);
        Assert.notEmpty(fields, "字段不能为空.");
        table.setName(tableName);
        table.setColumn(fields);
        table.setType(connectorService.getExtendedTableType().getCode());
        table.getExtInfo().put(HttpConstant.METHOD, method);
        table.getExtInfo().put(HttpConstant.API, api);
        table.getExtInfo().put(HttpConstant.CONTENT_TYPE, contentType);
        table.getExtInfo().put(HttpConstant.PARAMS, requestParams);
        return table;
    }

}