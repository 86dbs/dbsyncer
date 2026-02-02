/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.http.cdc.HttpQuartzListener;
import org.dbsyncer.connector.http.config.HttpConfig;
import org.dbsyncer.connector.http.schema.HttpSchemaResolver;
import org.dbsyncer.connector.http.validator.HttpConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public long getCount(HttpConnectorInstance connectorInstance, Map<String, String> command) {
        return 0;
    }

    @Override
    public Result reader(HttpConnectorInstance connectorInstance, ReaderContext context) {
        throw new HttpException("Http暂不支持全量读取");
    }

    @Override
    public Result writer(HttpConnectorInstance connectorInstance, PluginContext context) {
        List<Map> data = context.getTargetList();
        if (CollectionUtils.isEmpty(data)) {
            logger.error("writer data can not be empty.");
            throw new HttpException("writer data can not be empty.");
        }

        Result result = new Result();
        final List<Field> pkFields = PrimaryKeyUtil.findExistPrimaryKeyFields(context.getTargetFields());
        try {
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
        return new HashMap<>();
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        Map<String, String> cmd = new HashMap<>();
        return cmd;
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
}