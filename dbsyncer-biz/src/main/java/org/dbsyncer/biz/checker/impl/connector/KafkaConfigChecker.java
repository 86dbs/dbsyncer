package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.biz.checker.ConnectorConfigChecker;
import org.dbsyncer.common.util.BooleanUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.connector.config.KafkaConfig;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/22 23:55
 */
@Component
public class KafkaConfigChecker implements ConnectorConfigChecker<KafkaConfig> {

    @Override
    public void modify(KafkaConfig connectorConfig, Map<String, String> params) {
        String bootstrapServers = params.get("bootstrapServers");
        String keyDeserializer = params.get("keyDeserializer");
        String valueSerializer = params.get("valueSerializer");
        Assert.hasText(bootstrapServers, "bootstrapServers is empty.");
        Assert.hasText(keyDeserializer, "keyDeserializer is empty.");
        Assert.hasText(valueSerializer, "valueSerializer is empty.");

        String groupId = params.get("groupId");
        Assert.hasText(groupId, "groupId is empty.");
        boolean enableAutoCommit = BooleanUtil.toBoolean(params.get("enableAutoCommit"));
        long autoCommitIntervalMs = NumberUtil.toLong(params.get("autoCommitIntervalMs"));
        long maxPartitionFetchBytes = NumberUtil.toLong(params.get("maxPartitionFetchBytes"));

        String topic = params.get("topic");
        Assert.hasText(topic, "topic is empty.");
        long bufferMemory = NumberUtil.toLong(params.get("bufferMemory"));
        long batchSize = NumberUtil.toLong(params.get("batchSize"));
        long lingerMs = NumberUtil.toLong(params.get("lingerMs"));
        long maxBlockMs = NumberUtil.toLong(params.get("maxBlockMs"));
        long retries = NumberUtil.toLong(params.get("retries"));
        long retriesBackoffMs = NumberUtil.toLong(params.get("retriesBackoffMs"));
        long maxRequestSize = NumberUtil.toLong(params.get("maxRequestSize"));

        connectorConfig.setBootstrapServers(bootstrapServers);
        connectorConfig.setKeyDeserializer(keyDeserializer);
        connectorConfig.setValueSerializer(valueSerializer);

        connectorConfig.setGroupId(groupId);
        connectorConfig.setEnableAutoCommit(enableAutoCommit);
        connectorConfig.setAutoCommitIntervalMs(autoCommitIntervalMs);
        connectorConfig.setMaxPartitionFetchBytes(maxPartitionFetchBytes);

        connectorConfig.setTopic(topic);
        connectorConfig.setBufferMemory(bufferMemory);
        connectorConfig.setBatchSize(batchSize);
        connectorConfig.setLingerMs(lingerMs);
        connectorConfig.setMaxBlockMs(maxBlockMs);
        connectorConfig.setRetries(retries);
        connectorConfig.setRetriesBackoffMs(retriesBackoffMs);
        connectorConfig.setMaxRequestSize(maxRequestSize);
    }

}
