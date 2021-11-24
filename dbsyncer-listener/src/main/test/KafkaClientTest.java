import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/23 23:13
 */
public class KafkaClientTest {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;

    private String server = "192.168.1.100:9092";
    private String cKeyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    private String cValueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    private String pKeySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String pValueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String topic = "mytopic";

    @Before
    public void init() {
        // Consumer API
        {
            Properties props = new Properties();
            props.put("bootstrap.servers", server);
            props.put("group.id", "test");
            props.put("enable.auto.commit", true);
            props.put("auto.commit.interval.ms", 1000);
            props.put("session.timeout.ms", 10000);
            props.put("max.partition.fetch.bytes", 1048576);
            props.put("key.deserializer", cKeyDeserializer);
            props.put("value.deserializer", cValueDeserializer);
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topic));
        }

        // Producer API
        {
            Properties props = new Properties();
            props.put("bootstrap.servers", server);
            props.put("buffer.memory", 33554432);
            props.put("batch.size", 32768);
            props.put("linger.ms", 10);
            props.put("acks", "1");
            props.put("retries", 3);
            props.put("max.block.ms", 60000);
            props.put("max.request.size", 1048576);
            props.put("key.serializer", pKeySerializer);
            props.put("value.serializer", pValueSerializer);
            producer = new KafkaProducer<>(props);
        }
    }

    @After
    public void close() {
        if (null != producer) {
            producer.close();
        }
        if (null != consumer) {
            consumer.close();
        }
    }

    @Test
    public void testProducerAndConsumer() throws Exception {
        logger.info("test begin");
        new Producer().start();
        new Consumer().start();
        TimeUnit.SECONDS.sleep(60);
        logger.info("test end");
    }

    class Consumer extends Thread {

        public Consumer() {
            setName("Consumer-thread");
        }

        @Override
        public void run() {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("收到消息：offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
                }
            }
        }
    }

    class Producer extends Thread {

        public Producer() {
            setName("Producer-thread");
        }

        @Override
        public void run() {
            for (int i = 0; i < 100; i++) {
                producer.send(new ProducerRecord<>(topic, Integer.toString(i), "测试" + i));
            }
        }
    }

}
