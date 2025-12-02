/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka;

import org.apache.kafka.common.KafkaException;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.kafka.config.KafkaConfig;
import org.dbsyncer.connector.kafka.validator.KafkaConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.connector.AbstractConnector;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Kafka连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-11-22 23:55
 */
public class KafkaConnector extends AbstractConnector implements ConnectorService<KafkaConnectorInstance, KafkaConfig> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final KafkaConfigValidator configValidator = new KafkaConfigValidator();

    @Override
    public String getConnectorType() {
        return "Kafka";
    }

    @Override
    public Class<KafkaConfig> getConfigClass() {
        return KafkaConfig.class;
    }

    @Override
    public ConnectorInstance connect(KafkaConfig config, ConnectorServiceContext context) {
        return new KafkaConnectorInstance(config);
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public void disconnect(KafkaConnectorInstance connectorInstance) {
        connectorInstance.close();
    }

    @Override
    public boolean isAlive(KafkaConnectorInstance connectorInstance) {
        return connectorInstance.getConnection().ping();
    }

    @Override
    public List<Table> getTable(KafkaConnectorInstance connectorInstance, ConnectorServiceContext context) {
        List<Table> topics = new ArrayList<>();
        topics.add(new Table(connectorInstance.getConfig().getTopic()));
        return topics;
    }

    @Override
    public List<MetaInfo> getMetaInfo(KafkaConnectorInstance connectorInstance, ConnectorServiceContext context) {
        List<MetaInfo> metaInfos = new ArrayList<>();
        KafkaConfig config = connectorInstance.getConfig();
        List<Field> fields = JsonUtil.jsonToArray(config.getFields(), Field.class);
        metaInfos.add(new MetaInfo().setColumn(fields));
        return metaInfos;
    }

    @Override
    public long getCount(KafkaConnectorInstance connectorInstance, Map<String, String> command) {
        return 0;
    }

    @Override
    public Result reader(KafkaConnectorInstance connectorInstance, ReaderContext context) {
        throw new KafkaException("Full synchronization is not supported");
    }

    @Override
    public Result writer(KafkaConnectorInstance connectorInstance, PluginContext context) {
        List<Map> data = context.getTargetList();
        if (CollectionUtils.isEmpty(data)) {
            logger.error("writer data can not be empty.");
            throw new KafkaException("writer data can not be empty.");
        }

        Result result = new Result();
        final KafkaConfig cfg = connectorInstance.getConfig();
        final List<Field> pkFields = PrimaryKeyUtil.findExistPrimaryKeyFields(context.getTargetFields());
        try {
            String topic = cfg.getTopic();
            // 默认取第一个主键
            final String pk = pkFields.get(0).getName();
            data.forEach(row -> connectorInstance.getConnection().send(topic, String.valueOf(row.get(pk)), row));
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
        return Collections.EMPTY_MAP;
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        return Collections.EMPTY_MAP;
    }

    @Override
    public Listener getListener(String listenerType) {
        return null;
    }

}