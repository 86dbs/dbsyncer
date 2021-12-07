package org.dbsyncer.connector.kafka;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.util.KafkaUtil;

import java.util.List;
import java.util.Map;

public class KafkaConnector implements Connector<KafkaConnectorMapper, KafkaConfig> {
    @Override
    public ConnectorMapper connect(KafkaConfig config) {
        try {
            return new KafkaConnectorMapper(config, KafkaUtil.getConnection(config));
        } catch (Exception e) {
            throw new ConnectorException("无法连接, 请检查配置：" + e.getMessage());
        }
    }

    @Override
    public void disconnect(KafkaConnectorMapper connectorMapper) {
        KafkaUtil.close(connectorMapper.getConnection());
    }

    @Override
    public boolean isAlive(KafkaConnectorMapper connectorMapper) {
        return connectorMapper.getConnection().ping();
    }

    @Override
    public String getConnectorMapperCacheKey(KafkaConfig config) {
        return String.format("%s-%s", config.getBootstrapServers(), config.getGroupId());
    }

    @Override
    public List<Table> getTable(KafkaConnectorMapper connectorMapper) {
        return connectorMapper.getConnection().getTopics();
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