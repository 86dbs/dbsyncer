package org.dbsyncer.connector.es;

import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.elasticsearch.client.RestHighLevelClient;

public final class ESConnectorMapper implements ConnectorMapper<RestHighLevelClient> {
    private ConnectorConfig config;
    private RestHighLevelClient client;

    public ESConnectorMapper(ConnectorConfig config, RestHighLevelClient client) {
        this.config = config;
        this.client = client;
    }

    @Override
    public ConnectorConfig getConfig() {
        return config;
    }

    @Override
    public RestHighLevelClient getConnection() {
        return client;
    }
}