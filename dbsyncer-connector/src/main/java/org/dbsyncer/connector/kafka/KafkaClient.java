package org.dbsyncer.connector.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * // TODO implements Producer<?, ?>, Consumer<?>
 */
public class KafkaClient {

    private static final String DEFAULT_TOPIC = "__consumer_offsets";

    private KafkaConsumer consumer;
    private KafkaProducer producer;

    public List<Table> getTopics() {
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
    }

    public boolean ping() {
        return false;
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

    public void setConsumer(KafkaConsumer consumer) {
        this.consumer = consumer;
    }

    public KafkaProducer getProducer() {
        return producer;
    }

    public void setProducer(KafkaProducer producer) {
        this.producer = producer;
    }
}