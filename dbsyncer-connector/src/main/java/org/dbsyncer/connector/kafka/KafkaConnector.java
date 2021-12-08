package org.dbsyncer.connector.kafka;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.enums.KafkaFieldTypeEnum;
import org.dbsyncer.connector.util.KafkaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KafkaConnector implements Connector<KafkaConnectorMapper, KafkaConfig> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

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
        KafkaConfig config = connectorMapper.getConfig();
        List<Field> fields;
        try {
            String clazzName = config.getConsumerValueDeserializer();
            Class<?> clazz = Class.forName(clazzName);
            java.lang.reflect.Field[] clazzFields = clazz.getFields();
            if (CollectionUtils.isEmpty(clazzFields)) {
                throw new ConnectorException("查询字段不能为空.");
            }
            fields = Stream.of(clazzFields).map(f -> {
                String name = f.getName();
                String typeName = f.getType().getName();
                boolean pk = StringUtil.equals(config.getPrimaryKey(), name);
                return new Field(name, typeName, KafkaFieldTypeEnum.getType(typeName), pk);
            }).collect(Collectors.toList());
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e);
        }
        return new MetaInfo().setColumn(fields);
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
        return Collections.EMPTY_MAP;
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        return Collections.EMPTY_MAP;
    }
}