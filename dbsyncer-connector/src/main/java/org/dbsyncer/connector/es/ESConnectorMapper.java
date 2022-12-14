package org.dbsyncer.connector.es;

import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.config.ESConfig;
import org.dbsyncer.connector.util.ESUtil;
import org.elasticsearch.client.RestHighLevelClient;

public final class ESConnectorMapper implements ConnectorMapper<ESConfig, RestHighLevelClient> {
    private ESConfig config;
    private RestHighLevelClient client;

    public ESConnectorMapper(ESConfig config) {
        this.config = config;
        this.client = ESUtil.getConnection(config);
    }

    @Override
    public ESConfig getConfig() {
        return config;
    }

    @Override
    public void setConfig(ESConfig config) {
        this.config = config;
    }

    @Override
    public RestHighLevelClient getConnection() {
        return client;
    }

    @Override
    public void close() {
        ESUtil.close(client);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
