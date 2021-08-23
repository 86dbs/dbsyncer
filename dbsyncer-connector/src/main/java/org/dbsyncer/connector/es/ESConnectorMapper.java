package org.dbsyncer.connector.es;

import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.ESConfig;
import org.elasticsearch.client.RestHighLevelClient;

public final class ESConnectorMapper implements ConnectorMapper<ESConfig, RestHighLevelClient> {
    private ESConfig config;
    private RestHighLevelClient client;

    public ESConnectorMapper(ESConfig config, RestHighLevelClient client) {
        this.config = config;
        this.client = client;
    }

    @Override
    public ESConfig getConfig() {
        return config;
    }

    @Override
    public RestHighLevelClient getConnection() {
        return client;
    }
}