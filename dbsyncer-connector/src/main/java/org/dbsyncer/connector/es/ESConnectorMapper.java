package org.dbsyncer.connector.es;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.ESConfig;
import org.dbsyncer.connector.util.ESUtil;
import org.dbsyncer.sdk.spi.ConnectorMapper;
import org.elasticsearch.Version;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.MainResponse;

public final class ESConnectorMapper implements ConnectorMapper<ESConfig, EasyRestHighLevelClient> {
    private ESConfig config;
    private EasyRestHighLevelClient client;

    public ESConnectorMapper(ESConfig config) {
        this.config = config;
        this.client = ESUtil.getConnection(config);
        try {
            MainResponse info = client.info(RequestOptions.DEFAULT);
            client.setVersion(Version.fromString(info.getVersion().getNumber()));
        } catch (Exception e) {
            throw new ConnectorException(String.format("获取ES版本信息异常 %s, %s", config.getUrl(), e.getMessage()));
        }
    }

    @Override
    public String getServiceUrl() {
        return config.getUrl();
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
    public EasyRestHighLevelClient getConnection() {
        return client;
    }

    public Version getVersion() {
        return client.getVersion();
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