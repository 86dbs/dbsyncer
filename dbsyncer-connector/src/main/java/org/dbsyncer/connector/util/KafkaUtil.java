package org.dbsyncer.connector.util;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.connector.config.KafkaConfig;
import org.dbsyncer.connector.enums.KafkaFieldTypeEnum;
import org.dbsyncer.connector.enums.KafkaSerializerTypeEnum;
import org.dbsyncer.connector.kafka.KafkaClient;
import org.dbsyncer.connector.kafka.serialization.AbstractValueSerializer;
import org.dbsyncer.connector.kafka.serialization.JavaBeanSerializer;
import org.dbsyncer.connector.kafka.serialization.JsonSerializer;
import org.springframework.cglib.beans.BeanGenerator;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public abstract class KafkaUtil {

    private static Map<String, AbstractValueSerializer> map = new ConcurrentHashMap<>();

    static {
        map.put(KafkaSerializerTypeEnum.JSON.getCode(), new JsonSerializer());
        map.put(KafkaSerializerTypeEnum.JAVABEAN.getCode(), new JavaBeanSerializer());
    }

    public static KafkaClient getConnection(KafkaConfig config) {
        AbstractValueSerializer serializer = map.get(config.getSerializer());

        // Consumer API
        KafkaConsumer consumer;
        {
            Properties props = new Properties();
            props.put("bootstrap.servers", config.getBootstrapServers());
            props.put("group.id", config.getGroupId());
            props.put("enable.auto.commit", true);
            props.put("auto.commit.interval.ms", 5000);
            props.put("session.timeout.ms", config.getSessionTimeoutMs());
            props.put("max.partition.fetch.bytes", config.getMaxPartitionFetchBytes());
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", serializer.getDeserializer());
            consumer = new KafkaConsumer<>(props);
        }

        // Producer API
        KafkaProducer producer;
        {
            Properties props = new Properties();
            props.put("bootstrap.servers", config.getBootstrapServers());
            props.put("buffer.memory", config.getBufferMemory());
            props.put("batch.size", config.getBatchSize());
            props.put("linger.ms", config.getLingerMs());
            props.put("acks", config.getAcks());
            props.put("retries", config.getRetries());
            props.put("max.block.ms", 60000);
            props.put("max.request.size", config.getMaxRequestSize());
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", serializer.getSerializer());
            producer = new KafkaProducer<>(props);
        }

        List<Field> fields = JsonUtil.jsonToArray(config.getFields(), Field.class);
        BeanGenerator beanGenerator = new BeanGenerator();
        fields.forEach(f -> beanGenerator.addProperty(f.getName(), KafkaFieldTypeEnum.getType(f.getTypeName())));

        Object bean = beanGenerator.create();
        return new KafkaClient(consumer, producer, bean);
    }

    public static void close(KafkaClient client) {
        if (null != client) {
            client.close();
        }
    }

}