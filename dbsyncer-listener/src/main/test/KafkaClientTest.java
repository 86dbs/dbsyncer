import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.connector.config.KafkaConfig;
import org.dbsyncer.connector.enums.KafkaFieldTypeEnum;
import org.dbsyncer.connector.kafka.KafkaClient;
import org.dbsyncer.connector.kafka.serialization.JsonToMapDeserializer;
import org.dbsyncer.connector.kafka.serialization.MapToJsonSerializer;
import org.dbsyncer.connector.util.KafkaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/23 23:13
 */
public class KafkaClientTest {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private KafkaClient client;
    private KafkaConfig config;

    @Before
    public void init() {
        config = new KafkaConfig();
        config.setBootstrapServers("127.0.0.1:9092");
        config.setTopic("mytopic");
        config.setFields(getFields());
        config.setDeserializer(JsonToMapDeserializer.class.getName());
        config.setSerializer(MapToJsonSerializer.class.getName());

        config.setGroupId("test");
        config.setSessionTimeoutMs(10000);
        config.setMaxPartitionFetchBytes(1048576);

        config.setBufferMemory(33554432);
        config.setBatchSize(32768);
        config.setLingerMs(10);
        config.setAcks("1");
        config.setRetries(1);
        config.setMaxRequestSize(1048576);

        client = KafkaUtil.getConnection(config);
    }

    @After
    public void close() {
        client.close();
    }

    @Test
    public void testProducerAndConsumer() throws Exception {
        logger.info("test begin");
        logger.info("ping {}", client.ping());

        client.subscribe(Arrays.asList(config.getTopic()));

        // 模拟生产者
//        for (int i = 0; i < 5; i++) {
//            Map<String, Object> map = new HashMap<>();
//            map.put("id", i);
//            map.put("name", "张三" + i);
//            map.put("update_time", new Timestamp(System.currentTimeMillis()));
//            client.send(config.getTopic(), map.get("id").toString(), map);
//        }

        new Consumer().start();
        new Heartbeat().start();
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
                ConsumerRecords<String, Object> records = client.poll(100);
                for (ConsumerRecord record : records) {
                    logger.info("收到消息：offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
                }
            }
        }
    }

    class Heartbeat extends Thread {

        public Heartbeat() {
            setName("Heartbeat-thread");
        }

        @Override
        public void run() {
            while (true) {
                try {
                    TimeUnit.SECONDS.sleep(3L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                logger.info("ping {}", client.ping());
            }
        }
    }

}
