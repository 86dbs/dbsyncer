package org.dbsyncer.connector.config;

/**
 * @author AE86
 * @ClassName: KafkaConfig
 * @Description: Kafka连接配置
 * @date: 2021年11月4日 下午8:00:00
 */
public class KafkaConfig extends ConnectorConfig {

    /**
     * 消费者:
     * bootstrap.servers=192.168.100.1:9092,192.168.100.2:9092
     * # 消费者群组ID，发布-订阅模式，即如果一个生产者，多个消费者都要消费，那么需要定义自己的群组，同一群组内的消费者只有一个能消费到消息
     * group.id=my_group
     * # 如果为true，消费者的偏移量将在后台定期提交
     * enable.auto.commit=false
     * # 如何设置为自动提交（enable.auto.commit=true），这里设置自动提交周期
     * auto.commit.interval.ms=1000
     * # 在使用Kafka的组管理时，用于检测消费者故障的超时
     * session.timeout.ms=10000
     * key.deserializer=org.apache.kafka.common.serialization.StringSerializer
     * value.serializer=org.apache.kafka.common.serialization.StringSerializer
     *
     * 生产者:
     * # brokers集群
     * bootstrap.servers=192.168.100.1:9092,192.168.100.2:9092
     * acks=1
     * # 发送失败重试次数
     * retries=3
     * linger.ms=10
     * # 33554432 即32MB的批处理缓冲区
     * buffer.memory=33554432
     * # 批处理条数：当多个记录被发送到同一个分区时，生产者会尝试将记录合并到更少的请求中。这有助于客户端和服务器的性能
     * batch.size=32768
     * max.block.ms=500
     * key.deserializer=org.apache.kafka.common.serialization.StringSerializer
     * value.serializer=org.apache.kafka.common.serialization.StringSerializer
     */

    private String servers;
    private String groupId;
    private boolean autoCommit;
    private long autoCommitInterval;
    private long sessionTimeout;
    private long retries;
    private long linger;
    private long bufferMemory;
    private long batchSize;
    private long maxBlock;
    private String keyDeserializer;
    private String valueSerializer;

}