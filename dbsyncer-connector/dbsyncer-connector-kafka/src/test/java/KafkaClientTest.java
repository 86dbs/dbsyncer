/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.kafka.KafkaConnector;
import org.dbsyncer.connector.kafka.KafkaConnectorInstance;
import org.dbsyncer.connector.kafka.config.KafkaConfig;
import org.dbsyncer.connector.kafka.util.KafkaUtil;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/23 23:13
 */
public class KafkaClientTest {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private KafkaConnectorInstance instance;
    private KafkaConsumer<String, Object> consumer;
    private KafkaProducer<String, Object> producer;
    private final String topic = "my_topic";
    private final String groupId = "my_group";

    @Before
    public void init() {
        KafkaConfig config = new KafkaConfig();
        config.setUrl("127.0.0.1:9092");
        String properties = "connections.max.idle.ms=60000\n" +
                "request.timeout.ms=30000\n" +
                "retry.backoff.ms=100";
        config.getProperties().putAll(KafkaUtil.parse(properties));

        config.getExtInfo().put(KafkaUtil.CONSUMER_PROPERTIES, "key.deserializer=org.apache.kafka.common.serialization.StringDeserializer\n" +
                "value.deserializer=org.dbsyncer.connector.kafka.serialization.JsonToMapDeserializer\n" +
                "auto.offset.reset=latest\n" +
                "enable.auto.commit=false\n" +
                "auto.commit.interval.ms=5000\n" +
                "max.poll.records=500\n" +
                "max.poll.interval.ms=300000\n" +
                "max.partition.fetch.bytes=1048576\n" +
                "fetch.min.bytes=1\n" +
                "fetch.max.wait.ms=500\n" +
                "heartbeat.interval.ms=3000\n" +
                "session.timeout.ms=10000");

        config.getExtInfo().put(KafkaUtil.PRODUCER_PROPERTIES, "key.serializer=org.apache.kafka.common.serialization.StringSerializer\n" +
                "value.serializer=org.dbsyncer.connector.kafka.serialization.MapToJsonSerializer\n" +
                "acks=1\n" +
                "batch.size=32768\n" +
                "buffer.memory=33554432\n" +
                "compression.type=snappy\n" +
                "enable.idempotence=true\n" +
                "linger.ms=10\n" +
                "retries=1\n" +
                "max.block.ms=60000\n" +
                "max.request.size=1048576");
        instance = new KafkaConnectorInstance(config);

        // 生产者
        producer = instance.getProducer(topic);
        // 消费者
        consumer = instance.getConsumer(topic, groupId);
    }

    @After
    public void close() {
        if (consumer != null) {
            consumer.close();
        }
        if (instance != null) {
            instance.close();
        }
    }

    @Test
    public void testProducerAndConsumer() throws Exception {
        logger.info("test begin");
        KafkaConnector connector = new KafkaConnector();
        logger.info("ping {}", connector.isAlive(instance));

        // 模拟生产者
        for (int i = 0; i < 5; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", i);
            map.put("name", "张三" + i);
            map.put("update_time", new Timestamp(System.currentTimeMillis()));

            String key = String.valueOf(i);
            producer.send(new ProducerRecord<>(key, map));
        }

        new Consumer().start();
        TimeUnit.SECONDS.sleep(6000);
        logger.info("test end");
    }

    private String getFields() {
        List<Field> fields = new ArrayList<>();
        fields.add(genField("id", DataTypeEnum.STRING, true));
        fields.add(genField("name", DataTypeEnum.STRING));
        fields.add(genField("age", DataTypeEnum.INT));
        fields.add(genField("count", DataTypeEnum.LONG));
        fields.add(genField("type", DataTypeEnum.SHORT));
        fields.add(genField("money", DataTypeEnum.FLOAT));
        fields.add(genField("score", DataTypeEnum.DOUBLE));
        fields.add(genField("status", DataTypeEnum.BOOLEAN));
        fields.add(genField("create_date", DataTypeEnum.DATE));
        fields.add(genField("time", DataTypeEnum.TIME));
        fields.add(genField("update_time", DataTypeEnum.TIMESTAMP));
        return JsonUtil.objToJson(fields);
    }

    private Field genField(String name, DataTypeEnum typeEnum) {
        return genField(name, typeEnum, false);
    }

    private Field genField(String name, DataTypeEnum typeEnum, boolean pk) {
        return new Field(name, typeEnum.name(), 12, pk);
    }

    class Consumer extends Thread {

        public Consumer() {
            setName("Consumer-thread");
        }

        @Override
        public void run() {
            while (true) {
                ConsumerRecords<String, Object> records = consumer.poll(100);
                for (ConsumerRecord<String, Object> record : records) {
                    logger.info("收到消息：offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

}