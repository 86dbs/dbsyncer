/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.kafka.KafkaConnector;
import org.dbsyncer.connector.kafka.KafkaConnectorInstance;
import org.dbsyncer.connector.kafka.config.KafkaConfig;
import org.dbsyncer.connector.kafka.enums.KafkaFieldTypeEnum;
import org.dbsyncer.connector.kafka.util.KafkaUtil;
import org.dbsyncer.sdk.model.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/23 23:13
 */
public class KafkaClientTest {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private KafkaConnectorInstance instance;
    private KafkaConsumer consumer;

    @Before
    public void init() {
        KafkaConfig config = new KafkaConfig();
        config.setUrl("127.0.0.1:9092");
        String properties = "connections.max.idle.ms=60000\n" +
                "request.timeout.ms=30000\n" +
                "retry.backoff.ms=100";
        config.getProperties().putAll(KafkaUtil.parse(properties));
        config.getProperties().put("consumerProperties", "key.deserializer=org.apache.kafka.common.serialization.StringDeserializer\n" +
                "value.deserializer=org.dbsyncer.connector.kafka.serialization.JsonToMapDeserializer\n" +
                "session.timeout.ms=10000\n" +
                "max.partition.fetch.bytes=1048576\n" +
                "enable.auto.commit=true\n" +
                "auto.commit.interval.ms=5000");
        config.getProperties().put("producerProperties", "key.serializer=org.apache.kafka.common.serialization.StringSerializer\n" +
                "value.serializer=org.dbsyncer.connector.kafka.serialization.MapToJsonSerializer\n" +
                "buffer.memory=33554432\n" +
                "batch.size=32768\n" +
                "linger.ms=10\n" +
                "acks=1\n" +
                "retries=1\n" +
                "max.block.ms=60000\n" +
                "max.request.size=1048576");
        instance = new KafkaConnectorInstance(config);

        consumer = KafkaUtil.createConsumer(config, config.getProperties().getProperty("consumerProperties"));
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

//        instance.subscribe(Arrays.asList("my_topic"));

        // 模拟生产者
//        for (int i = 0; i < 5; i++) {
//            Map<String, Object> map = new HashMap<>();
//            map.put("id", i);
//            map.put("name", "张三" + i);
//            map.put("update_time", new Timestamp(System.currentTimeMillis()));
//            client.send(config.getTopic(), map.get("id").toString(), map);
//        }

        new Consumer().start();
        TimeUnit.SECONDS.sleep(6000);
        logger.info("test end");
    }

    private String getFields() {
        List<Field> fields = new ArrayList<>();
        fields.add(genField("id", KafkaFieldTypeEnum.STRING, true));
        fields.add(genField("name", KafkaFieldTypeEnum.STRING));
        fields.add(genField("age", KafkaFieldTypeEnum.INTEGER));
        fields.add(genField("count", KafkaFieldTypeEnum.LONG));
        fields.add(genField("type", KafkaFieldTypeEnum.SHORT));
        fields.add(genField("money", KafkaFieldTypeEnum.FLOAT));
        fields.add(genField("score", KafkaFieldTypeEnum.DOUBLE));
        fields.add(genField("status", KafkaFieldTypeEnum.BOOLEAN));
        fields.add(genField("create_date", KafkaFieldTypeEnum.DATE));
        fields.add(genField("time", KafkaFieldTypeEnum.TIME));
        fields.add(genField("update_time", KafkaFieldTypeEnum.TIMESTAMP));
        return JsonUtil.objToJson(fields);
    }

    private Field genField(String name, KafkaFieldTypeEnum typeEnum) {
        return genField(name, typeEnum, false);
    }

    private Field genField(String name, KafkaFieldTypeEnum typeEnum, boolean pk) {
        return new Field(name, typeEnum.getClazz().getSimpleName(), typeEnum.getType(), pk);
    }

    class Consumer extends Thread {

        public Consumer() {
            setName("Consumer-thread");
        }

        @Override
        public void run() {
            while (true) {
                ConsumerRecords<String, Object> records = consumer.poll(100);
                for (ConsumerRecord record : records) {
                    logger.info("收到消息：offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
                }
            }
        }
    }

}