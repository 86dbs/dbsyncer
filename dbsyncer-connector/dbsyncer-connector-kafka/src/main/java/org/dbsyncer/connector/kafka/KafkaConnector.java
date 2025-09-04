/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka;

import org.apache.kafka.common.KafkaException;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.kafka.config.KafkaConfig;
import org.dbsyncer.connector.kafka.validator.KafkaConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.connector.AbstractConnector;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
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

    private final KafkaConvertor kafkaConvertor = new KafkaConvertor();

    @Override
    public String getConnectorType() {
        return "Kafka";
    }

    @Override
    public Class<KafkaConfig> getConfigClass() {
        return KafkaConfig.class;
    }

    @Override
    public ConnectorInstance connect(KafkaConfig config) {
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
    public String getConnectorInstanceCacheKey(KafkaConfig config) {
        return String.format("%s-%s-%s", config.getConnectorType(), config.getBootstrapServers(), config.getGroupId());
    }

    @Override
    public List<Table> getTable(KafkaConnectorInstance connectorInstance) {
        return new ArrayList<>();
    }

    @Override
    public MetaInfo getMetaInfo(KafkaConnectorInstance connectorInstance, String tableName) {
        return new MetaInfo();
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
        final List<Field> pkFields = PrimaryKeyUtil.findExistPrimaryKeyFields(context.getTargetFields());
        try {
            // 从 command 中获取 topic 参数，如果没有则使用目标表名作为默认值
            String topic = context.getCommand().get("topic");

            topic = topic == null ? connectorInstance.getConfig().getTopic(): topic;

            // 验证 topic 是否为空
            if (topic == null || topic.isEmpty()) {
                throw new KafkaException("Topic is required");
            }

            // 默认取第一个主键
            final String pk = pkFields.get(0).getName();
            final KafkaClient kafkaClient = connectorInstance.getConnection();
            final String finalTopic = topic; // 创建final变量供lambda表达式使用

            // 格式化消息
            List<Map> formattedData = kafkaConvertor.formatMessages(context, data, pkFields);

            formattedData.forEach(row -> kafkaClient.send(finalTopic, String.valueOf(row.get(pk)), row));
            kafkaClient.producer.flush();
            logger.debug("----kafka wrote topic: {}, number: {}", finalTopic, formattedData.size());
            result.addSuccessData(formattedData);
        } catch (Exception e) {
            // 记录错误数据
            result.addFailData(data);
            result.getError().append(e.getMessage()).append(System.lineSeparator());
            logger.error(e.getMessage(), e);
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