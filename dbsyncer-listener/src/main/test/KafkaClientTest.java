import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.dbsyncer.connector.config.KafkaConfig;
import org.dbsyncer.connector.config.Table;
import org.dbsyncer.connector.kafka.KafkaClient;
import org.dbsyncer.connector.util.KafkaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/23 23:13
 */
public class KafkaClientTest {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private KafkaClient client;

    @Before
    public void init() {
        String cKeyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
        String cValueDeserializer = cKeyDeserializer;
        String pKeySerializer = "org.apache.kafka.common.serialization.StringSerializer";
        String pValueSerializer = pKeySerializer;

        KafkaConfig config = new KafkaConfig();
        config.setBootstrapServers("192.168.1.100:9092");

        config.setGroupId("test");
        config.setConsumerKeyDeserializer(cKeyDeserializer);
        config.setConsumerValueDeserializer(cValueDeserializer);
        config.setSessionTimeoutMs(10000);
        config.setMaxPartitionFetchBytes(1048576);

        config.setProducerKeySerializer(pKeySerializer);
        config.setProducerValueSerializer(pValueSerializer);
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
        List<Table> topics = client.getTopics();
        topics.forEach(t -> logger.info(t.getName()));

        logger.info("ping {}", client.ping());

        String topic = "mytopic";
        logger.info("Subscribed to topic {}", topic);
        client.getConsumer().subscribe(Arrays.asList(topic));

        // 模拟生产者
        for (int i = 0; i < 1; i++) {
            client.getProducer().send(new ProducerRecord<>(topic, Integer.toString(i), "测试" + i));
        }

        new Consumer().start();
        new Heartbeat().start();
        TimeUnit.SECONDS.sleep(600);
        logger.info("test end");
    }

    class Consumer extends Thread {

        public Consumer() {
            setName("Consumer-thread");
        }

        @Override
        public void run() {
            while (true) {
                ConsumerRecords<String, String> records = client.getConsumer().poll(100);
                for (ConsumerRecord<String, String> record : records) {
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
