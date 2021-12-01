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
    private int sessionTimeoutMs;
    private int maxPartitionFetchBytes;

    // 生产者
    private String producerKeySerializer;
    private String producerValueSerializer;
    private int bufferMemory;
    private int batchSize;
    private int lingerMs;
    private String acks;
    private int retries;
    private int maxRequestSize;

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

    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public void setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public int getMaxPartitionFetchBytes() {
        return maxPartitionFetchBytes;
    }

    public void setMaxPartitionFetchBytes(int maxPartitionFetchBytes) {
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

    public int getBufferMemory() {
        return bufferMemory;
    }

    public void setBufferMemory(int bufferMemory) {
        this.bufferMemory = bufferMemory;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getLingerMs() {
        return lingerMs;
    }

    public void setLingerMs(int lingerMs) {
        this.lingerMs = lingerMs;
    }

    public String getAcks() {
        return acks;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public int getMaxRequestSize() {
        return maxRequestSize;
    }

    public void setMaxRequestSize(int maxRequestSize) {
        this.maxRequestSize = maxRequestSize;
    }
}