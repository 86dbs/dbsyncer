package org.dbsyncer.connector.es;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.AbstractConnector;
import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.util.ESUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class ESConnector extends AbstractConnector implements Connector<ESConnectorMapper, ESConfig> {

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
        return String.format("%s-%s", config.getUrl(), config.getUsername());
    }

    @Override
    public List<String> getTable(ESConnectorMapper connectorMapper) {
        try {
            ESConfig config = connectorMapper.getConfig();
            GetIndexRequest request = new GetIndexRequest();
            GetIndexResponse indexResponse = connectorMapper.getConnection().indices().get(request, RequestOptions.DEFAULT);
            MappingMetaData mappingMetaData = indexResponse.getMappings().get(config.getIndex());
            List<String> tables = new ArrayList<>();
            tables.add(mappingMetaData.type());
            return tables;
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e);
        }
    }

    @Override
    public MetaInfo getMetaInfo(ESConnectorMapper connectorMapper, String tableName) {
        ESConfig config = connectorMapper.getConfig();
        List<Field> fields = new ArrayList<>();
        try {
            GetIndexRequest request = new GetIndexRequest(config.getIndex());
            GetIndexResponse indexResponse = connectorMapper.getConnection().indices().get(request, RequestOptions.DEFAULT);
            MappingMetaData mappingMetaData = indexResponse.getMappings().get(config.getIndex());
            Map<String, Object> propertiesMap = mappingMetaData.getSourceAsMap();
            Map<String, Map> properties = (Map<String, Map>) propertiesMap.get(ESUtil.PROPERTIES);
            if (CollectionUtils.isEmpty(properties)) {
                throw new ConnectorException("查询字段不能为空.");
            }
            properties.keySet().forEach(k -> {
                // TODO 获取实现类型
                //String columnType = (String) v.get("type");
                fields.add(new Field(k, k, 12, false));
            });
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e);
        }
        return new MetaInfo().setColumn(fields);
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
    public long getCount(ESConnectorMapper connectorMapper, Map<String, String> command) {
        try {
            ESConfig config = connectorMapper.getConfig();
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.trackTotalHits(true);
            sourceBuilder.from(0);
            sourceBuilder.size(0);
            SearchRequest request = new SearchRequest(new String[]{config.getIndex()}, sourceBuilder);
            SearchResponse response = connectorMapper.getConnection().search(request, RequestOptions.DEFAULT);
            return response.getHits().getTotalHits();
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e);
        }
    }

    @Override
    public Result reader(ESConnectorMapper connectorMapper, ReaderConfig config) {
        return null;
    }

    @Override
    public Result writer(ESConnectorMapper connectorMapper, WriterBatchConfig config) {
        List<Map> data = config.getData();
        if (CollectionUtils.isEmpty(data) || CollectionUtils.isEmpty(config.getFields())) {
            logger.error("writer data can not be empty.");
            throw new ConnectorException("writer data can not be empty.");
        }

        Result result = new Result();
        final ESConfig cfg = connectorMapper.getConfig();
        Field pkField = getPrimaryKeyField(config.getFields());
        try {
            BulkRequest request = new BulkRequest();
            data.forEach(row -> {
                IndexRequest r = new IndexRequest(cfg.getIndex(), cfg.getType(), String.valueOf(row.get(pkField.getName())));
                r.source(row, XContentType.JSON);
                request.add(r);
            });

            BulkResponse response = connectorMapper.getConnection().bulk(request, RequestOptions.DEFAULT);
            RestStatus restStatus = response.status();
            if (restStatus.getStatus() != RestStatus.OK.getStatus()) {
                throw new ConnectorException(String.format("error code:%s", restStatus.getStatus()));
            }
        } catch (IOException e) {
            // 记录错误数据
            result.getFailData().addAll(data);
            result.getFail().set(data.size());
            result.getError().append(e.getMessage()).append(System.lineSeparator());
            logger.error(e.getMessage());
        }
        return result;
    }

    @Override
    public Result writer(ESConnectorMapper connectorMapper, WriterSingleConfig config) {
        Map<String, Object> data = config.getData();
        Field pkField = getPrimaryKeyField(config.getFields());
        String pk = String.valueOf(data.get(pkField.getName()));

        if (isUpdate(config.getEvent())) {
            return execute(connectorMapper, data, pk, (index, type, id) -> {
                UpdateRequest request = new UpdateRequest(index, type, id);
                request.doc(data, XContentType.JSON);
                return connectorMapper.getConnection().update(request, RequestOptions.DEFAULT);
            });
        }
        if (isInsert(config.getEvent())) {
            return execute(connectorMapper, data, pk, (index, type, id) -> {
                IndexRequest request = new IndexRequest(index, type, id);
                request.source(data, XContentType.JSON);
                return connectorMapper.getConnection().index(request, RequestOptions.DEFAULT);
            });
        }
        if (isDelete(config.getEvent())) {
            return execute(connectorMapper, data, pk, (index, type, id) ->
                    connectorMapper.getConnection().delete(new DeleteRequest(index, type, id), RequestOptions.DEFAULT)
            );
        }

        throw new ConnectorException("Unsupported event");
    }

    private Result execute(ESConnectorMapper connectorMapper, Map<String, Object> data, String id, RequestMapper mapper) {
        Result result = new Result();
        final ESConfig config = connectorMapper.getConfig();
        try {
            StatusToXContentObject status = mapper.apply(config.getIndex(), config.getType(), id);
            RestStatus restStatus = status.status();
            if (restStatus.getStatus() != RestStatus.OK.getStatus()) {
                throw new ConnectorException(String.format("error code:%s", restStatus.getStatus()));
            }
        } catch (Exception e) {
            // 记录错误数据
            result.getFailData().add(data);
            result.getFail().set(1);
            result.getError().append("INDEX:").append(config.getIndex()).append(System.lineSeparator())
                    .append("TYPE:").append(config.getType()).append(System.lineSeparator())
                    .append("ID:").append(id).append(System.lineSeparator())
                    .append("DATA:").append(data).append(System.lineSeparator())
                    .append("ERROR:").append(e.getMessage()).append(System.lineSeparator());
            logger.error("INDEX:{}, TYPE:{}, ID:{}, DATA:{}, ERROR:{}", config.getIndex(), config.getType(), id, data, e.getMessage());
        }
        return result;
    }

    private interface RequestMapper {
        StatusToXContentObject apply(String index, String type, String id) throws IOException;
    }
}