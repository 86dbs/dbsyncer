package org.dbsyncer.connector.kafka;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.AbstractConnector;
import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.KafkaConfig;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.config.WriterBatchConfig;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.connector.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KafkaConnector extends AbstractConnector implements Connector<KafkaConnectorMapper, KafkaConfig> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public ConnectorMapper connect(KafkaConfig config) {
        try {
            return new KafkaConnectorMapper(config);
        } catch (Exception e) {
            throw new ConnectorException("无法连接, 请检查配置：" + e.getMessage());
        }
    }

    @Override
    public void disconnect(KafkaConnectorMapper connectorMapper) {
        connectorMapper.close();
    }

    @Override
    public boolean isAlive(KafkaConnectorMapper connectorMapper) {
        return connectorMapper.getConnection().ping();
    }

    @Override
    public String getConnectorMapperCacheKey(KafkaConfig config) {
        return String.format("%s-%s-%s-%s", config.getConnectorType(), config.getBootstrapServers(), config.getTopic(), config.getGroupId());
    }

    @Override
    public List<Table> getTable(KafkaConnectorMapper connectorMapper) {
        List<Table> topics = new ArrayList<>();
        topics.add(new Table(connectorMapper.getConfig().getTopic()));
        return topics;
    }

    @Override
    public MetaInfo getMetaInfo(KafkaConnectorMapper connectorMapper, String tableName) {
        KafkaConfig config = connectorMapper.getConfig();
        List<Field> fields = JsonUtil.jsonToArray(config.getFields(), Field.class);
        return new MetaInfo().setColumn(fields);
    }

    @Override
    public long getCount(KafkaConnectorMapper connectorMapper, Map<String, String> command) {
        return 0;
    }

    @Override
    public Result reader(KafkaConnectorMapper connectorMapper, ReaderConfig config) {
        throw new ConnectorException("Full synchronization is not supported");
    }

    @Override
    public Result writer(KafkaConnectorMapper connectorMapper, WriterBatchConfig config) {
        List<Map> data = config.getData();
        if (CollectionUtils.isEmpty(data) || CollectionUtils.isEmpty(config.getFields())) {
            logger.error("writer data can not be empty.");
            throw new ConnectorException("writer data can not be empty.");
        }

        Result result = new Result();
        final KafkaConfig cfg = connectorMapper.getConfig();
        final List<Field> pkFields = PrimaryKeyUtil.findConfigPrimaryKeyFields(config);
        try {
            String topic = cfg.getTopic();
            // 默认取第一个主键
            final String pk = pkFields.get(0).getName();
            data.forEach(row -> connectorMapper.getConnection().send(topic, String.valueOf(row.get(pk)), row));
            result.addSuccessData(data);
        } catch (Exception e) {
            // 记录错误数据
            result.addFailData(data);
            result.getError().append(e.getMessage()).append(System.lineSeparator());
            logger.error(e.getMessage());
        }
        return result;
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