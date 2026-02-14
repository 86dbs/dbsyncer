/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONPath;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.http.cdc.HttpQuartzListener;
import org.dbsyncer.connector.http.config.HttpConfig;
import org.dbsyncer.connector.http.constant.HttpConstant;
import org.dbsyncer.connector.http.enums.ContentTypeEnum;
import org.dbsyncer.connector.http.enums.HttpMethodEnum;
import org.dbsyncer.connector.http.model.HttpResponse;
import org.dbsyncer.connector.http.model.RequestBuilder;
import org.dbsyncer.connector.http.schema.HttpSchemaResolver;
import org.dbsyncer.connector.http.validator.HttpConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.MetaContext;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Http连接器实现
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-02 00:01
 */
public class HttpConnector implements ConnectorService<HttpConnectorInstance, HttpConfig> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final HttpConfigValidator configValidator = new HttpConfigValidator();
    private final HttpSchemaResolver schemaResolver = new HttpSchemaResolver();

    @Override
    public String getConnectorType() {
        return "Http";
    }

    @Override
    public TableTypeEnum getExtendedTableType() {
        return TableTypeEnum.SEMI;
    }

    @Override
    public Class<HttpConfig> getConfigClass() {
        return HttpConfig.class;
    }

    @Override
    public ConnectorInstance connect(HttpConfig config, ConnectorServiceContext context) {
        return new HttpConnectorInstance(config);
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public void disconnect(HttpConnectorInstance connectorInstance) {
        connectorInstance.close();
    }

    @Override
    public boolean isAlive(HttpConnectorInstance connectorInstance) {
        try {
            return true;
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            throw new HttpException(e);
        }
    }

    @Override
    public List<String> getDatabases(HttpConnectorInstance connectorInstance) {
        return new ArrayList<>();
    }

    @Override
    public List<Table> getTable(HttpConnectorInstance connectorInstance, ConnectorServiceContext context) {
        return new ArrayList<>();
    }

    @Override
    public List<MetaInfo> getMetaInfo(HttpConnectorInstance connectorInstance, ConnectorServiceContext context) {
        List<MetaInfo> metaInfos = new ArrayList<>();
        for (Table table : context.getTablePatterns()) {
            MetaInfo metaInfo = new MetaInfo();
            metaInfo.setTable(table.getName());
            metaInfo.setTableType(getExtendedTableType().getCode());
            metaInfo.setColumn(table.getColumn());
            metaInfo.setExtInfo(table.getExtInfo());
            metaInfos.add(metaInfo);
        }
        return metaInfos;
    }

    @Override
    public long getCount(HttpConnectorInstance connectorInstance, MetaContext metaContext) {
        Table sourceTable = metaContext.getSourceTable();
        Map<String, Object> params = getParams(sourceTable.getExtInfo(), 1, 1, null);
        RequestBuilder builder = genRequestBuilder(connectorInstance, sourceTable, params);
        HttpResponse<String> execute = builder.execute();
        if (!execute.isSuccess()) {
            String errMsg = StringUtil.isNotBlank(execute.getMessage()) ? execute.getMessage()
                    : (StringUtil.isNotBlank(execute.getBody()) ? execute.getBody() : "Request failed, status: " + execute.getStatusCode());
            throw new HttpException(errMsg);
        }
        String data = execute.getBody();
        if (StringUtil.isBlank(data)) {
            return 0;
        }
        try {
            Object rootObject = JSON.parse(data);
            if (rootObject == null) {
                return 0;
            }
            String extractTotal = sourceTable.getExtInfo().getProperty(HttpConstant.EXTRACT_TOTAL);
            Object total = JSONPath.eval(rootObject, extractTotal);
            if (total instanceof Number) {
                return ((Number) total).longValue();
            }
        } catch (Exception e) {
            logger.error("Failed to extract total from response: {}", e.getMessage());
            throw new HttpException("Failed to extract total: " + (e.getMessage() != null ? e.getMessage() : data));
        }
        return 0;
    }

    @Override
    public Result reader(HttpConnectorInstance connectorInstance, ReaderContext context) {
        Table sourceTable = context.getSourceTable();
        Map<String, Object> params = getParams(sourceTable.getExtInfo(), context.getPageIndex(), context.getPageSize(), context.getCursors());
        RequestBuilder builder = genRequestBuilder(connectorInstance, sourceTable, params);
        HttpResponse<String> execute = builder.execute();
        String data = execute.getBody();
        if (StringUtil.isNotBlank(data)) {
            Object rootObject = JSON.parse(data);
            if (rootObject != null) {
                String extractData = sourceTable.getExtInfo().getProperty(HttpConstant.EXTRACT_PATH);
                Object list = JSONPath.eval(rootObject, extractData);
                if (list instanceof JSONArray) {
                    JSONArray jsonArray = (JSONArray) list;
                    return new Result(jsonArray.toList(Map.class));
                }
            }
        }
        return new Result();
    }

    @Override
    public Result writer(HttpConnectorInstance connectorInstance, PluginContext context) {
        List<Map> data = context.getTargetList();
        if (CollectionUtils.isEmpty(data)) {
            logger.error("writer data can not be empty.");
            throw new HttpException("writer data can not be empty.");
        }

        Result result = new Result();
        try {
            Table targetTable = context.getTargetTable();
            // []
            String params = targetTable.getExtInfo().getProperty(HttpConstant.PARAMS);
            Assert.hasText(params, "params can not be empty.");
            RequestBuilder builder = genRequestBuilder(connectorInstance, targetTable);

            // 解析请求体模板为 JSON 对象
            Object rootJSON = JSON.parse(params);
            // 将 data 转换为 JSONArray
            JSONArray dataArray = JSON.parseArray(JSON.toJSONString(data));
            if (rootJSON instanceof JSONArray) {
                // 特殊处理：如果模板是数组
                ((JSONArray) rootJSON).clear();
                ((JSONArray) rootJSON).addAll(dataArray);
            } else {
                // 一般情况：使用 JSONPath.set 通过 writePath 路径设置值, 如 $.data，用于定位要替换的位置
                String writePath = targetTable.getExtInfo().getProperty(HttpConstant.WRITE_PATH);
                Assert.hasText(writePath, "writePath can not be empty.");
                JSONPath.set(rootJSON, writePath, dataArray);
            }

            builder.setBodyAsJsonString(JSON.toJSONString(rootJSON));
            HttpResponse<String> execute = builder.execute();
            if (execute.isSuccess()) {
                result.addSuccessData(data);
                return result;
            }
            // 异常信息可能在 message 或 body 中，优先使用有内容的
            String errMsg = StringUtil.isNotBlank(execute.getMessage()) ? execute.getMessage()
                    : (StringUtil.isNotBlank(execute.getBody()) ? execute.getBody() : "Request failed, status: " + execute.getStatusCode());
            throw new HttpException(errMsg);
        } catch (Exception e) {
            // 记录错误数据
            result.addFailData(data);
            result.getError().append(e.getMessage()).append(System.lineSeparator());
            logger.error(e.getMessage());
        }
        return result;
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        return new HashMap<>();
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        return new HashMap<>();
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new HttpQuartzListener();
        }
        return null;
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    private RequestBuilder genRequestBuilder(HttpConnectorInstance connectorInstance, Table sourceTable) {
        String serviceUrl = connectorInstance.getServiceUrl();
        String api = sourceTable.getExtInfo().getProperty(HttpConstant.API);
        String method = sourceTable.getExtInfo().getProperty(HttpConstant.METHOD);
        String url = serviceUrl;
        if (!url.endsWith("/")) {
            url += "/";
        }
        url += api.startsWith("/") ? api.substring(1) : api;
        HttpMethodEnum httpMethod = HttpMethodEnum.valueOf(method);

        Assert.notNull(httpMethod, "method can not be null");
        return new RequestBuilder(connectorInstance.getConnection(), url, httpMethod);
    }

    private RequestBuilder genRequestBuilder(HttpConnectorInstance connectorInstance, Table sourceTable, Map<String, Object> params) {
        String contentType = sourceTable.getExtInfo().getProperty(HttpConstant.CONTENT_TYPE);
        ContentTypeEnum contentTypeEnum = ContentTypeEnum.fromValue(contentType);
        Assert.notNull(contentTypeEnum, "content type can not be null");
        RequestBuilder builder = genRequestBuilder(connectorInstance, sourceTable);
        builder.setContentType(contentTypeEnum);
        if (contentTypeEnum == ContentTypeEnum.JSON) {
            builder.setBodyAsJson(params);
        } else {
            builder.addBodyParams(params);
        }
        return builder;
    }

    private Map<String, Object> getParams(Properties extInfo, int pageIndex, int pageSize, Object[] cursors) {
        Properties properties = PropertiesUtil.parse(extInfo.getProperty(HttpConstant.PARAMS));
        String pageIndexKey = "";
        String pageSizeKey = "";
        List<String> cursorKeys = null;
        Map<String, Object> params = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            String val = (String) entry.getValue();
            if (StringUtil.equalsIgnoreCase(HttpConstant.PAGE_INDEX, val)) {
                pageIndexKey = key;
                continue;
            }
            if (StringUtil.equalsIgnoreCase(HttpConstant.PAGE_SIZE, val)) {
                pageSizeKey = key;
                continue;
            }
            if (StringUtil.equalsIgnoreCase(HttpConstant.CURSOR, val)) {
                if (cursorKeys == null) {
                    cursorKeys = new ArrayList<>();
                }
                cursorKeys.add(key);
                continue;
            }
            params.put(key, val);
        }

        if (StringUtil.isNotBlank(pageIndexKey)) {
            params.put(pageIndexKey, pageIndex);
        }
        if (StringUtil.isNotBlank(pageSizeKey)) {
            params.put(pageSizeKey, pageSize);
        }
        if (cursorKeys != null && cursors != null && cursors.length > 0) {
            Assert.isTrue(cursorKeys.size() == cursors.length, "游标参数不一致");
            for (int i = 0; i < cursors.length; i++) {
                params.put(cursorKeys.get(i), cursors[i]);
            }
        }
        return params;
    }
}