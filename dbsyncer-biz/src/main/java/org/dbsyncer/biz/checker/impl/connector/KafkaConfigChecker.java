package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.biz.checker.ConnectorConfigChecker;
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
        Assert.hasText(bootstrapServers, "bootstrapServers is empty.");

        String groupId = params.get("groupId");
        String consumerKeyDeserializer = params.get("consumerKeyDeserializer");
        String consumerValueDeserializer = params.get("consumerValueDeserializer");
        Assert.hasText(consumerKeyDeserializer, "consumerKeyDeserializer is empty.");
        Assert.hasText(consumerValueDeserializer, "consumerValueDeserializer is empty.");
        int sessionTimeoutMs = NumberUtil.toInt(params.get("sessionTimeoutMs"));
        int maxPartitionFetchBytes = NumberUtil.toInt(params.get("maxPartitionFetchBytes"));

        String producerKeySerializer = params.get("producerKeySerializer");
        String producerValueSerializer = params.get("producerValueSerializer");
        String acks = params.get("acks");
        Assert.hasText(producerKeySerializer, "producerKeySerializer is empty.");
        Assert.hasText(producerValueSerializer, "producerValueSerializer is empty.");
        int bufferMemory = NumberUtil.toInt(params.get("bufferMemory"));
        int batchSize = NumberUtil.toInt(params.get("batchSize"));
        int lingerMs = NumberUtil.toInt(params.get("lingerMs"));
        int retries = NumberUtil.toInt(params.get("retries"));
        int maxRequestSize = NumberUtil.toInt(params.get("maxRequestSize"));

        connectorConfig.setBootstrapServers(bootstrapServers);

        connectorConfig.setGroupId(groupId);
        connectorConfig.setConsumerKeyDeserializer(consumerKeyDeserializer);
        connectorConfig.setConsumerValueDeserializer(consumerValueDeserializer);
        connectorConfig.setSessionTimeoutMs(sessionTimeoutMs);
        connectorConfig.setMaxPartitionFetchBytes(maxPartitionFetchBytes);

        connectorConfig.setProducerKeySerializer(producerKeySerializer);
        connectorConfig.setProducerValueSerializer(producerValueSerializer);
        connectorConfig.setBufferMemory(bufferMemory);
        connectorConfig.setBatchSize(batchSize);
        connectorConfig.setLingerMs(lingerMs);
        connectorConfig.setAcks(acks);
        connectorConfig.setRetries(retries);
        connectorConfig.setMaxRequestSize(maxRequestSize);
    }

}
