package org.dbsyncer.connector.kafka;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.*;

import java.util.List;
import java.util.Map;

public class KafkaConnector implements Connector<KafkaConnectorMapper, KafkaConfig> {
    @Override
    public ConnectorMapper connect(KafkaConfig config) {
        return new KafkaConnectorMapper(config, new KafkaClient());
    }

    @Override
    public void disconnect(KafkaConnectorMapper connectorMapper) {

    }

    @Override
    public boolean isAlive(KafkaConnectorMapper connectorMapper) {
        return false;
    }

    @Override
    public String getConnectorMapperCacheKey(KafkaConfig config) {
        return String.format("%s-%s", config.getBootstrapServers(), config.getGroupId());
    }

    @Override
    public List<Table> getTable(KafkaConnectorMapper connectorMapper) {
        return null;
    }

    @Override
    public MetaInfo getMetaInfo(KafkaConnectorMapper connectorMapper, String tableName) {
        return null;
    }

    @Override
    public long getCount(KafkaConnectorMapper connectorMapper, Map<String, String> command) {
        return 0;
    }

    @Override
    public Result reader(KafkaConnectorMapper connectorMapper, ReaderConfig config) {
        return null;
    }

    @Override
    public Result writer(KafkaConnectorMapper connectorMapper, WriterBatchConfig config) {
        return null;
    }

    @Override
    public Result writer(KafkaConnectorMapper connectorMapper, WriterSingleConfig config) {
        return null;
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        return null;
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        return null;
    }
}