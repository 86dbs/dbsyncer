/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka;

import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaException;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.kafka.config.KafkaConfig;
import org.dbsyncer.connector.kafka.validator.KafkaConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.connector.AbstractConnector;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.enums.TableTypeEnum;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Kafka连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-11-22 23:55
 */
public class KafkaConnector extends AbstractConnector implements ConnectorService<KafkaConnectorInstance, KafkaConfig> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public static final String TOPIC = "topic";
    public static final String PRODUCER_PROPERTIES = "producerProperties";
    public static final String GROUP_ID = "group.id";

    private final KafkaConfigValidator configValidator = new KafkaConfigValidator();

    @Override
    public String getConnectorType() {
        return "Kafka";
    }

    @Override
    public TableTypeEnum getExtendedTableType() {
        return TableTypeEnum.SEMI;
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
        try {
            connectorInstance.getConnection().describeCluster()
                    .clusterId()
                    .get(5, TimeUnit.SECONDS); // 5秒超时
            return true;
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public List<String> getDatabases(KafkaConnectorInstance connectorInstance) {
        try {
            DescribeClusterResult clusterResult = connectorInstance.getConnection().describeCluster();
            String clusterId = clusterResult.clusterId().get();
            return Collections.singletonList(clusterId);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public List<Table> getTable(KafkaConnectorInstance connectorInstance, ConnectorServiceContext context) {
        try {
            List<Table> tables = new ArrayList<>();
            Set<String> topics = connectorInstance.getConnection().listTopics().names().get();
            if (!CollectionUtils.isEmpty(topics)) {
                topics.forEach(topic -> {
                    Table t = new Table();
                    t.setName(topic);
                    t.setType(getExtendedTableType().getCode());
                    t.setColumn(new ArrayList<>());
                    tables.add(t);
                });
            }
            return tables;
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public List<MetaInfo> getMetaInfo(KafkaConnectorInstance connectorInstance, ConnectorServiceContext context) {
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
    public long getCount(KafkaConnectorInstance connectorInstance, Map<String, String> command) {
        return 0;
    }

    @Override
    public Result reader(KafkaConnectorInstance connectorInstance, ReaderContext context) {
        throw new KafkaException("Unsupported method");
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
            String topic = context.getCommand().get(TOPIC);
            String producerProperties = context.getCommand().get(PRODUCER_PROPERTIES);
            connectorInstance.checkProducerConfig(topic, producerProperties);
            String key = StringUtil.join(pkFields, StringUtil.UNDERLINE);
            data.forEach(row -> connectorInstance.send(topic, key, row));
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
        Map<String, String> cmd = new HashMap<>();
        cmd.put(TOPIC, commandConfig.getTable().getName());
        cmd.put(GROUP_ID, String.valueOf(commandConfig.getTable().getExtInfo().get(GROUP_ID)));
        return cmd;
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        Map<String, String> cmd = new HashMap<>();
        cmd.put(TOPIC, commandConfig.getTable().getName());
        return cmd;
    }

    @Override
    public Listener getListener(String listenerType) {
        return null;
    }

}