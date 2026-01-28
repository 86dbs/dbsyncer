/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka;

import org.apache.kafka.common.KafkaException;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.kafka.config.KafkaConfig;
import org.dbsyncer.connector.kafka.model.KafkaMessage;
import org.dbsyncer.connector.kafka.validator.KafkaConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DDLConfig;
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

import java.util.*;

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
        // Kafka 作为目标连接器时，字段结构由源表决定，这里返回空的字段列表
        // 避免后续使用时出现 NullPointerException
        return new MetaInfo().setColumn(new ArrayList<>());
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

            topic = topic == null ? connectorInstance.getConfig().getTopic() : topic;

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
            result.addFailData(context.getSourceList());  // 存储源数据，便于重试时直接使用
            result.error = e.getMessage();
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

    @Override
    public Result insert(KafkaConnectorInstance connectorInstance, PluginContext context) {
        // INSERT 操作：使用所有字段
        return writer(connectorInstance, context);
    }

    @Override
    public Result update(KafkaConnectorInstance connectorInstance, PluginContext context) {
        // INSERT 操作：使用所有字段
        return writer(connectorInstance, context);
    }

    @Override
    public Result delete(KafkaConnectorInstance connectorInstance, PluginContext context) {
        // INSERT 操作：使用所有字段
        return writer(connectorInstance, context);
    }

    @Override
    public Result upsert(KafkaConnectorInstance connectorInstance, PluginContext context) {
        // INSERT 操作：使用所有字段
        return writer(connectorInstance, context);
    }

    @Override
    public Result writerDDL(KafkaConnectorInstance connectorInstance, DDLConfig ddlConfig, org.dbsyncer.sdk.plugin.PluginContext context) {
        Result result = new Result();
        try {
            String ddlSql = ddlConfig.getSql();
            assert ddlConfig != null && !StringUtil.isBlank(ddlSql);

            logger.info("准备发送 DDL 消息到 Kafka: sql={}", ddlSql);

            // 从context中获取表名（与DML处理方式一致）
            assert context != null;
            String tableName = context.getSourceTableName();

            // 获取Topic名称（使用配置中的topic）
            KafkaConfig config = connectorInstance.getConfig();
            String topic = config.getTopic();
            if (StringUtil.isBlank(topic)) {
                throw new KafkaException("Topic配置不能为空");
            }

            // 从context中获取源连接器信息（类似DML处理方式）
            String sourceConnectorType = context.getSourceConnectorInstance().getConfig().getConnectorType();
            String sourceAddress = kafkaConvertor.getSourceAddressFromSource(context);
            String sourceDBName = kafkaConvertor.getSourceDBNameFromSource(context);

            // 格式化DDL消息
            KafkaMessage message = KafkaMessage.createDDLMessage(
                ddlConfig, tableName, sourceConnectorType, sourceAddress, sourceDBName);

            // 发送到Kafka（使用表名作为Key，确保同一表的DDL和DML消息有序）
            KafkaClient kafkaClient = connectorInstance.getConnection();
            kafkaClient.send(topic, tableName, message.toMap());
            kafkaClient.producer.flush();

            logger.info("DDL 消息发送成功: topic={}, table={}, operation={}",
                    topic, tableName, ddlConfig.getDdlOperationEnum());

            // 返回成功结果
            Map<String, String> successMap = new HashMap<>();
            successMap.put("ddlSql", ddlSql);
            result.addSuccessData(Collections.singletonList(successMap));
        } catch (Exception e) {
            logger.error("发送DDL消息到Kafka失败: sql={}, error={}",
                    ddlConfig != null ? ddlConfig.getSql() : "null", e.getMessage(), e);
            // 返回失败结果
            Map<String, Object> failMap = new HashMap<>();
            if (ddlConfig != null) {
                failMap.put("ddlSql", ddlConfig.getSql());
            }
            result.addFailData(Collections.singletonList(failMap));
            result.error = String.format("发送DDL消息失败: %s", e.getMessage());
        }
        return result;
    }


}