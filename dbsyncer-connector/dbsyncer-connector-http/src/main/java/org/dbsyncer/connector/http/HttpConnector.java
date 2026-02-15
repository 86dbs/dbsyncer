/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONPath;
import org.dbsyncer.common.model.OpenApiData;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.http.cdc.HttpQuartzListener;
import org.dbsyncer.connector.http.config.HttpConfig;
import org.dbsyncer.connector.http.constant.HttpConstant;
import org.dbsyncer.connector.http.enums.ContentTypeEnum;
import org.dbsyncer.connector.http.enums.HttpMethodEnum;
import org.dbsyncer.connector.http.model.HttpResponse;
import org.dbsyncer.connector.http.model.RequestBuilder;
import org.dbsyncer.connector.http.schema.HttpSchemaResolver;
import org.dbsyncer.connector.http.util.HttpUtil;
import org.dbsyncer.connector.http.validator.HttpConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.BaseContext;
import org.dbsyncer.sdk.plugin.MetaContext;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.spi.ConnectorService;
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
    public long getCount(HttpConnectorInstance connectorInstance, MetaContext context) {
        Table sourceTable = context.getSourceTable();
        Map<String, Object> params = getCountParams(sourceTable.getExtInfo());
        RequestBuilder builder = buildRequestBuilder(connectorInstance, context, sourceTable, params);
        HttpResponse execute = builder.execute();
        String data = execute.getBody();
        if (StringUtil.isNotBlank(data)) {
            Object rootObject = JSON.parse(data);
            if (rootObject != null) {
                String extractTotal = sourceTable.getExtInfo().getProperty(HttpConstant.EXTRACT_TOTAL);
                Object total = JSONPath.eval(rootObject, extractTotal);
                if (total instanceof Number) {
                    return ((Number) total).longValue();
                }
            }
        }
        return 0;
    }

    @Override
    public Result reader(HttpConnectorInstance connectorInstance, ReaderContext context) {
        Table sourceTable = context.getSourceTable();
        Map<String, Object> params = getParams(context);
        RequestBuilder builder = buildRequestBuilder(connectorInstance, context, sourceTable, params);
        HttpResponse execute = builder.execute();
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
            RequestBuilder builder = genRequestBuilder(connectorInstance, context.getTargetTable());
            // 解析请求体模板为 JSON 对象
            Object dataObj = writeData(connectorInstance, context, data);
            builder.setBodyAsJsonString(JSON.toJSONString(dataObj));
            builder.execute();
            result.addSuccessData(data);
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
        Map<String, String> map = new HashMap<>();
        map.put(ConnectorConstant.OPERTION_QUERY, "HTTP");
        return map;
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

    private RequestBuilder buildRequestBuilder(HttpConnectorInstance connectorInstance, BaseContext context, Table sourceTable, Map<String, Object> params) {
        if (connectorInstance.getConfig().isEnableEncrypt() && context.getRsaManager() != null && context.getRsaConfig() != null) {
            boolean publicNetwork = connectorInstance.getConfig().isPublicNetwork();
            OpenApiData dataObj = context.getRsaManager().encrypt(context.getRsaConfig(), params, publicNetwork);
            RequestBuilder builder = genRequestBuilder(connectorInstance, sourceTable);
            builder.setBodyAsJsonString(JSON.toJSONString(dataObj));
            return builder;
        }

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

    private Object writeData(HttpConnectorInstance connectorInstance, PluginContext context, List<Map> data) {
        if (connectorInstance.getConfig().isEnableEncrypt() && context.getRsaManager() != null && context.getRsaConfig() != null) {
            boolean publicNetwork = connectorInstance.getConfig().isPublicNetwork();
            return context.getRsaManager().encrypt(context.getRsaConfig(), data, publicNetwork);
        }

        // []
        Table targetTable = context.getTargetTable();
        String params = targetTable.getExtInfo().getProperty(HttpConstant.PARAMS);
        Assert.hasText(params, "params can not be empty.");
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
        return rootJSON;
    }

    private RequestBuilder genRequestBuilder(HttpConnectorInstance connectorInstance, Table sourceTable) {
        String serviceUrl = connectorInstance.getServiceUrl();
        String api = sourceTable.getExtInfo().getProperty(HttpConstant.API);
        String method = sourceTable.getExtInfo().getProperty(HttpConstant.METHOD);
        String url = serviceUrl;
        if (!url.endsWith("/")) {
            url += "/";
        }
        url += api != null && api.startsWith("/") ? api.substring(1) : (api != null ? api : "");
        HttpMethodEnum httpMethod = HttpMethodEnum.valueOf(method);

        Assert.notNull(httpMethod, "method can not be null");
        RequestBuilder builder = new RequestBuilder(connectorInstance.getConnection(), url, httpMethod);

        // 获取连接参数
        HttpConfig config = connectorInstance.getConfig();
        if (config != null && config.getProperties() != null) {
            Properties props = config.getProperties();
            int connectionTimeout = NumberUtil.toInt(props.getProperty(HttpConstant.CONNECTION_TIMEOUT_MS), 30000);
            int socketTimeout = NumberUtil.toInt(props.getProperty(HttpConstant.SOCKET_TIMEOUT_MS), 30000);
            int connectionRequestTimeout = NumberUtil.toInt(props.getProperty(HttpConstant.CONNECTION_REQUEST_TIMEOUT_MS), 30000);
            int retryTimes = NumberUtil.toInt(props.getProperty(HttpConstant.RETRY_TIMES), 0);
            String charset = props.getProperty(HttpConstant.CHARSET, "UTF-8");
            builder.setConnectionTimeout(connectionTimeout);
            builder.setSocketTimeout(socketTimeout);
            builder.setConnectionRequestTimeout(connectionRequestTimeout);
            builder.setRetryTimes(retryTimes);
            builder.setCharset(charset);
        }
        return builder;
    }

    /**
     * 统一增量参数替换。从 context 的 command 中读取静态变量（时间/日期），
     * 与 pageIndex、pageSize、cursors 一起做一次占位符替换。
     */
    private Map<String, Object> getParams(ReaderContext context) {
        Properties extInfo = context.getSourceTable().getExtInfo();
        String paramsTemplate = extInfo.getProperty(HttpConstant.PARAMS);
        if (StringUtil.isBlank(paramsTemplate)) {
            return new HashMap<>();
        }
        Properties template = HttpUtil.parse(paramsTemplate);
        Map<String, Object> varMap = new HashMap<>();
        varMap.put(HttpConstant.PAGE_INDEX, context.getPageIndex());
        varMap.put(HttpConstant.PAGE_SIZE, context.getPageSize());
        String staticVarsJson = context.getCommand() != null ? context.getCommand().get(HttpConstant.HTTP_INCREMENT_VARS) : null;
        if (StringUtil.isNotBlank(staticVarsJson)) {
            try {
                JSONObject jo = JSON.parseObject(staticVarsJson);
                if (jo != null) {
                    for (String k : jo.keySet()) {
                        varMap.put(k, jo.get(k));
                    }
                }
            } catch (Exception e) {
                logger.warn("Parse HTTP_INCREMENT_VARS failed: {}", e.getMessage());
            }
        }

        List<String> cursorKeys = new ArrayList<>();
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<Object, Object> entry : template.entrySet()) {
            String key = (String) entry.getKey();
            String raw = (String) entry.getValue();
            String val = raw != null ? StringUtil.trim(raw) : "";
            if (HttpConstant.CURSOR.equals(val)) {
                cursorKeys.add(key);
                continue;
            }
            result.put(key, varMap.getOrDefault(val, val));
        }
        Object[] cursors = context.getCursors();
        if (!cursorKeys.isEmpty() && cursors != null) {
            for (int i = 0; i < cursorKeys.size() && i < cursors.length; i++) {
                result.put(cursorKeys.get(i), cursors[i]);
            }
        }
        return result;
    }

    /** 供 getCount 等无 ReaderContext 场景使用 */
    private Map<String, Object> getCountParams(Properties extInfo) {
        String paramsTemplate = extInfo.getProperty(HttpConstant.PARAMS);
        if (StringUtil.isBlank(paramsTemplate)) {
            return new HashMap<>();
        }
        Properties template = HttpUtil.parse(paramsTemplate);
        String pageIndexKey = "";
        String pageSizeKey = "";
        Map<String, Object> params = new HashMap<>();
        for (Map.Entry<Object, Object> entry : template.entrySet()) {
            String key = (String) entry.getKey();
            String val = (String) entry.getValue();
            if (HttpConstant.PAGE_INDEX.equals(val)) {
                pageIndexKey = key;
                continue;
            }
            if (HttpConstant.PAGE_SIZE.equals(val)) {
                pageSizeKey = key;
                continue;
            }
            params.put(key, val);
        }
        if (StringUtil.isNotBlank(pageIndexKey)) {
            params.put(pageIndexKey, 1);
        }
        if (StringUtil.isNotBlank(pageSizeKey)) {
            params.put(pageSizeKey, 1);
        }
        return params;
    }
}