package org.dbsyncer.connector.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Kafka客户端，集成消费者、生产者API
 */
public class KafkaClient {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String DEFAULT_TOPIC = "__consumer_offsets";

    private KafkaConsumer consumer;
    private KafkaProducer producer;
    private NetworkClient networkClient;

    public KafkaClient(KafkaConsumer consumer, KafkaProducer producer) {
        this.consumer = consumer;
        this.producer = producer;
    }

    public List<Table> getTopics() {
        try {
            Map<String, List<PartitionInfo>> topicMap = consumer.listTopics();
            List<Table> topics = new ArrayList<>();
            if (!CollectionUtils.isEmpty(topicMap)) {
                topicMap.forEach((t, list) -> {
                    if (!StringUtil.equals(DEFAULT_TOPIC, t)) {
                        topics.add(new Table(t));
                    }
                });
            }
            return topics;
        } catch (Exception e) {
            throw new ConnectorException("获取Topic异常" + e.getMessage());
        }
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

//    private boolean isConnected(Object obj, String nodeId) throws NoSuchFieldException, InvocationTargetException, IllegalAccessException {
//        final Field field = obj.getClass().getDeclaredField("connectionStates");
//        field.setAccessible(true);
//        Object connectionStates = field.get(obj);
//
//        Method[] methods = connectionStates.getClass().getDeclaredMethods();
//        Method method = null;
//        for (int i = 0; i < methods.length; i++) {
//            if (methods[i].getName() == "isConnected") {
//                method = methods[i];
//                method.setAccessible(true);
//                break;
//            }
//        }
//        return (boolean) method.invoke(connectionStates, nodeId);
//    }

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