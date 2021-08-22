package org.dbsyncer.connector.es;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.*;

import java.util.List;
import java.util.Map;

public final class ESConnector implements Connector {

    @Override
    public ConnectorMapper connect(ConnectorConfig config) {
        return null;
    }

    @Override
    public void disconnect(ConnectorMapper connectorMapper) {

    }

    @Override
    public boolean isAlive(ConnectorMapper connectorMapper) {
        return false;
    }

    @Override
    public String getConnectorMapperCacheKey(ConnectorConfig config) {
        return null;
    }

    @Override
    public List<String> getTable(ConnectorMapper connectorMapper) {
        return null;
    }

    @Override
    public MetaInfo getMetaInfo(ConnectorMapper connectorMapper, String tableName) {
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
    public long getCount(ConnectorMapper config, Map<String, String> command) {
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
