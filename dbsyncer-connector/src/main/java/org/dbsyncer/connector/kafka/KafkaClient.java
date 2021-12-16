package org.dbsyncer.connector.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Node;
import org.dbsyncer.connector.ConnectorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;

/**
 * Kafka客户端，集成消费者、生产者API
 */
public class KafkaClient {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private KafkaConsumer consumer;
    private KafkaProducer producer;
    private NetworkClient networkClient;
    // 序列化/反序列化对象
    private Object serializationObject;

    public KafkaClient(KafkaConsumer consumer, KafkaProducer producer, Object serializationObject) {
        this.consumer = consumer;
        this.producer = producer;
        this.serializationObject = serializationObject;
    }

    public boolean ping() {
        return ping(consumer);
    }

    private boolean ping(Object client) {
        if (null == networkClient) {
            synchronized (this) {
                if (null == networkClient) {
                    try {
                        networkClient = (NetworkClient) invoke(invoke(client, "client"), "client");
                    } catch (NoSuchFieldException e) {
                        logger.error(e.getMessage());
                    } catch (IllegalAccessException e) {
                        logger.error(e.getMessage());
                    }
                }
            }
        }
        final Node node = networkClient.leastLoadedNode(0);
        InetSocketAddress address = new InetSocketAddress(node.host(), node.port());
        if (address.isUnresolved()) {
            throw new ConnectorException(String.format("DNS resolution failed for url in %s %s:%s", CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, node.host(), node.port()));
        }
        return true;
    }

    private Object invoke(Object obj, String declaredFieldName) throws NoSuchFieldException, IllegalAccessException {
        final Field field = obj.getClass().getDeclaredField(declaredFieldName);
        field.setAccessible(true);
        return field.get(obj);
    }

    public void close() {
        if (null != producer) {
            producer.close();
        }
        if (null != consumer) {
            consumer.close();
        }
    }

    public KafkaConsumer getConsumer() {
        return consumer;
    }

    public KafkaProducer getProducer() {
        return producer;
    }
}