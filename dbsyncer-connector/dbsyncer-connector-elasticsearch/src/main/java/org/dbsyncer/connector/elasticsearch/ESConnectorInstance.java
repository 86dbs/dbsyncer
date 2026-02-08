/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch;

import org.dbsyncer.connector.elasticsearch.api.EasyRestHighLevelClient;
import org.dbsyncer.connector.elasticsearch.config.ESConfig;
import org.dbsyncer.connector.elasticsearch.util.ESUtil;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.MainResponse;

/**
 * ES连接器实例
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-11-25 23:10
 */
public final class ESConnectorInstance implements ConnectorInstance<ESConfig, EasyRestHighLevelClient> {

    private ESConfig config;
    private EasyRestHighLevelClient client;

    public ESConnectorInstance(ESConfig config) {
        this.config = config;
        this.client = ESUtil.getConnection(config);
        try {
            MainResponse info = client.info(RequestOptions.DEFAULT);
            client.setVersion(Version.fromString(info.getVersion().getNumber()));
        } catch (Exception e) {
            throw new ElasticsearchException(String.format("获取ES版本信息异常 %s, %s", config.getUrl(), e.getMessage()));
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