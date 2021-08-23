package org.dbsyncer.connector.util;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.ESConfig;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.connector.config.MetaInfo;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;

import java.io.IOException;
import java.util.*;

public abstract class ESUtil {

    public static final String PROPERTIES = "properties";
    private static final int ADDRESS_LENGTH = 2;

    private ESUtil() {
    }

    public static RestHighLevelClient getConnection(ESConfig config) {
        String[] ipAddress = StringUtils.split(config.getClusterNodes(), ",");
        HttpHost[] hosts = Arrays.stream(ipAddress).map(node -> makeHttpHost(node, config.getScheme())).filter(Objects::nonNull).toArray(
                HttpHost[]::new);
        RestClientBuilder builder = RestClient.builder(hosts);
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        Credentials credentials = new UsernamePasswordCredentials(config.getUsername(), config.getPassword());
        credentialsProvider.setCredentials(AuthScope.ANY, credentials);
        builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(builder);
    }

    public static void close(RestHighLevelClient client) {
        if (null != client) {
            try {
                client.close();
            } catch (IOException e) {
                throw new ConnectorException(e.getMessage());
            }
        }
    }

    /**
     * 获取元数据信息
     *
     * @param client
     * @param index
     * @param type
     * @return
     */
    public static MetaInfo getMetaInfo(RestHighLevelClient client, String index, String type) throws IOException {
        GetIndexRequest request = new GetIndexRequest();
        request.indices(index);
        GetIndexResponse indexResponse = client.indices().get(request, RequestOptions.DEFAULT);

        // 字段信息
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = indexResponse.getMappings();
        ImmutableOpenMap<String, MappingMetaData> indexMap = mappings.get(index);
        MappingMetaData mappingMetaData = indexMap.get(type);
        Map<String, Object> propertiesMap = mappingMetaData.getSourceAsMap();
        Map<String, Map> properties = (Map<String, Map>) propertiesMap.get(PROPERTIES);
        if (CollectionUtils.isEmpty(properties)) {
            throw new ConnectorException("查询字段不能为空.");
        }

        List<Field> fields = new ArrayList<>();
        properties.forEach((k, v) -> {
            // TODO 获取实现类型
            //String columnType = (String) v.get("type");
            fields.add(new Field(k, k, 12, false));
        });
        return new MetaInfo().setColumn(fields);
    }

    /**
     * 根据配置创建HttpHost
     *
     * @param address
     * @param scheme
     * @return
     */
    private static HttpHost makeHttpHost(String address, String scheme) {
        String[] arr = address.split(":");
        if (arr.length == ADDRESS_LENGTH) {
            String ip = arr[0];
            int port = Integer.parseInt(arr[1]);
            return new HttpHost(ip, port, scheme);
        } else {
            return null;
        }
    }
}