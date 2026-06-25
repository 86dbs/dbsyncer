/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.rocketmq;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.rocketmq.cdc.RocketMQListener;
import org.dbsyncer.connector.rocketmq.config.RocketMQConfig;
import org.dbsyncer.connector.rocketmq.constant.RocketMQConstant;
import org.dbsyncer.connector.rocketmq.schema.RocketMQSchemaResolver;
import org.dbsyncer.connector.rocketmq.util.RocketMQMessageUtil;
import org.dbsyncer.connector.rocketmq.validator.RocketMQConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.connector.AbstractConnector;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.MetaContext;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * RocketMQ 连接器实现
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 01:00
 */
public class RocketMQConnector extends AbstractConnector implements ConnectorService<RocketMQConnectorInstance, RocketMQConfig> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final RocketMQConfigValidator configValidator = new RocketMQConfigValidator();
    private final RocketMQSchemaResolver schemaResolver = new RocketMQSchemaResolver();

    @Override
    public String getConnectorType() {
        return "RocketMQ";
    }

    @Override
    public TableTypeEnum getExtendedTableType() {
        return TableTypeEnum.SEMI;
    }

    @Override
    public Class<RocketMQConfig> getConfigClass() {
        return RocketMQConfig.class;
    }

    @Override
    public ConnectorInstance connect(RocketMQConfig config, ConnectorServiceContext context) {
        return new RocketMQConnectorInstance(config);
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public void disconnect(RocketMQConnectorInstance connectorInstance) {
        connectorInstance.close();
    }

    @Override
    public boolean isAlive(RocketMQConnectorInstance connectorInstance) {
        try {
            connectorInstance.ping();
            return true;
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            throw new RocketMQException(e);
        }
    }

    @Override
    public List<String> getDatabases(RocketMQConnectorInstance connectorInstance) {
        List<String> databases = new ArrayList<>();
        String url = connectorInstance.getServiceUrl();
        if (StringUtil.isNotBlank(url)) {
            databases.add(url);
        }
        return databases;
    }

    @Override
    public List<Table> getTable(RocketMQConnectorInstance connectorInstance, ConnectorServiceContext context) {
        return new ArrayList<>();
    }

    @Override
    public List<MetaInfo> getMetaInfo(RocketMQConnectorInstance connectorInstance, ConnectorServiceContext context) {
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
    public long getCount(RocketMQConnectorInstance connectorInstance, MetaContext metaContext) {
        return 0;
    }

    @Override
    public Result reader(RocketMQConnectorInstance connectorInstance, ReaderContext context) {
        throw new RocketMQException("RocketMQ暂不支持全量读取");
    }

    @Override
    public Result writer(RocketMQConnectorInstance connectorInstance, PluginContext context) {
        List<Map> data = context.getTargetList();
        if (CollectionUtils.isEmpty(data)) {
            logger.error("writer data can not be empty.");
            throw new RocketMQException("writer data can not be empty.");
        }

        Result result = new Result();
        List<Field> pkFields = PrimaryKeyUtil.findExistPrimaryKeyFields(context.getTargetFields());
        String topic = context.getCommand().get(RocketMQConstant.TOPIC);
        String tags = context.getCommand().get(RocketMQConstant.TAGS);
        if (StringUtil.isBlank(tags)) {
            tags = "*";
        }
        String sourceTableName = context.getSourceTable().getName();
        String event = StringUtil.isBlank(context.getEvent()) ? ConnectorConstant.OPERTION_INSERT : context.getEvent();
        try {
            DefaultMQProducer producer = connectorInstance.getProducer();
            for (Map row : data) {
                try {
                    Map<String, Object> message = RocketMQMessageUtil.buildMessage(sourceTableName, event, row);
                    String key = RocketMQMessageUtil.buildMessageKey(pkFields, row);
                    byte[] body = JsonUtil.objToJsonSafe(message).getBytes(StandardCharsets.UTF_8);
                    Message rocketMessage = new Message(topic, tags, key, body);
                    SendResult sendResult = producer.send(rocketMessage);
                    if (sendResult == null) {
                        throw new RocketMQException("RocketMQ 发送结果为空");
                    }
                    result.getSuccessData().add(row);
                } catch (Exception e) {
                    result.getFailData().add(row);
                    result.getError().append(e.getMessage()).append(System.lineSeparator());
                    logger.error(e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            result.addFailData(data);
            result.getError().append(e.getMessage()).append(System.lineSeparator());
            logger.error(e.getMessage(), e);
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
        Table table = commandConfig.getTable();
        cmd.put(RocketMQConstant.TOPIC, table.getName());
        String tags = table.getExtInfo().getProperty("tags");
        if (StringUtil.isNotBlank(tags)) {
            cmd.put(RocketMQConstant.TAGS, tags);
        }
        return cmd;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isLog(listenerType)) {
            return new RocketMQListener();
        }
        return null;
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }
}
