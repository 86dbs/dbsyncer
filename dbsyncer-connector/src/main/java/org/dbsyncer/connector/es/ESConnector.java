package org.dbsyncer.connector.es;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.util.ESUtil;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class ESConnector implements Connector<ESConnectorMapper, ESConfig> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public ConnectorMapper connect(ESConfig config) {
        return new ESConnectorMapper(config, ESUtil.getConnection(config));
    }

    @Override
    public void disconnect(ESConnectorMapper connectorMapper) {
        ESUtil.close(connectorMapper.getConnection());
    }

    @Override
    public boolean isAlive(ESConnectorMapper connectorMapper) {
        try {
            RestHighLevelClient client = connectorMapper.getConnection();
            return client.ping(RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    @Override
    public String getConnectorMapperCacheKey(ESConfig config) {
        return String.format("%s-%s", config.getClusterNodes(), config.getUsername());
    }

    @Override
    public List<String> getTable(ESConnectorMapper connectorMapper) {
        return null;
    }

    @Override
    public MetaInfo getMetaInfo(ESConnectorMapper connectorMapper, String tableName) {
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

    @Override
    public long getCount(ESConnectorMapper config, Map<String, String> command) {
        return 0;
    }

    @Override
    public Result reader(ReaderConfig config) {
        return null;
    }

    @Override
    public Result writer(WriterBatchConfig config) {
        return null;
    }

    @Override
    public Result writer(WriterSingleConfig config) {
        return null;
    }
}