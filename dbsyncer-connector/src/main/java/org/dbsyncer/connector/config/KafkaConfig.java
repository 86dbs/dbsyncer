package org.dbsyncer.connector.config;

/**
 * @author AE86
 * @ClassName: KafkaConfig
 * @Description: Kafka连接配置
 * @date: 2021年11月4日 下午8:00:00
 */
public class KafkaConfig extends ConnectorConfig {

    private String bootstrapServers;

    // 消费者
    private String groupId;
    private String consumerKeyDeserializer;
    private String consumerValueDeserializer;
    private boolean enableAutoCommit;
    private long autoCommitIntervalMs;
    private long maxPartitionFetchBytes;

    // 生产者
    private String producerKeySerializer;
    private String producerValueSerializer;
    private long bufferMemory;
    private long batchSize;
    private long lingerMs;
    private long maxBlockMs;
    private long retries;
    private long retriesBackoffMs;
    private long maxRequestSize;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getConsumerKeyDeserializer() {
        return consumerKeyDeserializer;
    }

    public void setConsumerKeyDeserializer(String consumerKeyDeserializer) {
        this.consumerKeyDeserializer = consumerKeyDeserializer;
    }

    public String getConsumerValueDeserializer() {
        return consumerValueDeserializer;
    }

    public void setConsumerValueDeserializer(String consumerValueDeserializer) {
        this.consumerValueDeserializer = consumerValueDeserializer;
    }

    public boolean isEnableAutoCommit() {
        return enableAutoCommit;
    }

    public void setEnableAutoCommit(boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    public long getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public void setAutoCommitIntervalMs(long autoCommitIntervalMs) {
        this.autoCommitIntervalMs = autoCommitIntervalMs;
    }

    public long getMaxPartitionFetchBytes() {
        return maxPartitionFetchBytes;
    }

    public void setMaxPartitionFetchBytes(long maxPartitionFetchBytes) {
        this.maxPartitionFetchBytes = maxPartitionFetchBytes;
    }

    public String getProducerKeySerializer() {
        return producerKeySerializer;
    }

    public void setProducerKeySerializer(String producerKeySerializer) {
        this.producerKeySerializer = producerKeySerializer;
    }

    public String getProducerValueSerializer() {
        return producerValueSerializer;
    }

    public void setProducerValueSerializer(String producerValueSerializer) {
        this.producerValueSerializer = producerValueSerializer;
    }

    public long getBufferMemory() {
        return bufferMemory;
    }

    public void setBufferMemory(long bufferMemory) {
        this.bufferMemory = bufferMemory;
    }

    public long getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    public long getLingerMs() {
        return lingerMs;
    }

    public void setLingerMs(long lingerMs) {
        this.lingerMs = lingerMs;
    }

    public long getMaxBlockMs() {
        return maxBlockMs;
    }

    public void setMaxBlockMs(long maxBlockMs) {
        this.maxBlockMs = maxBlockMs;
    }

    public long getRetries() {
        return retries;
    }

    public void setRetries(long retries) {
        this.retries = retries;
    }

    public long getRetriesBackoffMs() {
        return retriesBackoffMs;
    }

    public void setRetriesBackoffMs(long retriesBackoffMs) {
        this.retriesBackoffMs = retriesBackoffMs;
    }

    public long getMaxRequestSize() {
        return maxRequestSize;
    }

    public void setMaxRequestSize(long maxRequestSize) {
        this.maxRequestSize = maxRequestSize;
    }
}