package org.dbsyncer.connector.es;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.AbstractConnector;
import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.ESConfig;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.config.WriterBatchConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.enums.ESFieldTypeEnum;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.Filter;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.Table;
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
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class ESConnector extends AbstractConnector implements Connector<ESConnectorMapper, ESConfig> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static Map<String, FilterMapper> filters = new LinkedHashMap<>();

    static {
        filters.putIfAbsent(FilterEnum.EQUAL.getName(), (builder, k, v) -> builder.must(QueryBuilders.matchQuery(k, v)));
        filters.putIfAbsent(FilterEnum.NOT_EQUAL.getName(), (builder, k, v) -> builder.mustNot(QueryBuilders.matchQuery(k, v)));
        filters.putIfAbsent(FilterEnum.GT.getName(), (builder, k, v) -> builder.filter(QueryBuilders.rangeQuery(k).gt(v)));
        filters.putIfAbsent(FilterEnum.LT.getName(), (builder, k, v) -> builder.filter(QueryBuilders.rangeQuery(k).lt(v)));
        filters.putIfAbsent(FilterEnum.GT_AND_EQUAL.getName(), (builder, k, v) -> builder.filter(QueryBuilders.rangeQuery(k).gte(v)));
        filters.putIfAbsent(FilterEnum.LT_AND_EQUAL.getName(), (builder, k, v) -> builder.filter(QueryBuilders.rangeQuery(k).lte(v)));
        filters.putIfAbsent(FilterEnum.LIKE.getName(), (builder, k, v) -> builder.filter(QueryBuilders.wildcardQuery(k, v)));
    }

    @Override
    public ConnectorMapper connect(ESConfig config) {
        return new ESConnectorMapper(config);
    }

    @Override
    public void disconnect(ESConnectorMapper connectorMapper) {
        connectorMapper.close();
    }

    @Override
    public boolean isAlive(ESConnectorMapper connectorMapper) {
        try {
            RestHighLevelClient client = connectorMapper.getConnection();
            return client.ping(RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e.getMessage());
        }
    }

    @Override
    public String getConnectorMapperCacheKey(ESConfig config) {
        return String.format("%s-%s-%s-%s", config.getConnectorType(), config.getUrl(), config.getIndex(), config.getUsername());
    }

    @Override
    public List<Table> getTable(ESConnectorMapper connectorMapper) {
        try {
            ESConfig config = connectorMapper.getConfig();
            GetIndexRequest request = new GetIndexRequest(config.getIndex());
            GetIndexResponse indexResponse = connectorMapper.getConnection().indices().get(request, RequestOptions.DEFAULT);
            MappingMetadata mappingMetaData = indexResponse.getMappings().get(config.getIndex());
            List<Table> tables = new ArrayList<>();
            tables.add(new Table(mappingMetaData.type()));
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
            MappingMetadata mappingMetaData = indexResponse.getMappings().get(config.getIndex());
            Map<String, Object> propertiesMap = mappingMetaData.getSourceAsMap();
            Map<String, Map> properties = (Map<String, Map>) propertiesMap.get(ESUtil.PROPERTIES);
            if (CollectionUtils.isEmpty(properties)) {
                throw new ConnectorException("查询字段不能为空.");
            }
            properties.forEach((k, v) -> {
                String columnType = (String) v.get("type");
                fields.add(new Field(k, columnType, ESFieldTypeEnum.getType(columnType), StringUtil.equals(config.getPrimaryKey(), k)));
            });
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e);
        }
        return new MetaInfo().setColumn(fields);
    }

    @Override
    public long getCount(ESConnectorMapper connectorMapper, Map<String, String> command) {
        try {
            ESConfig config = connectorMapper.getConfig();
            SearchSourceBuilder builder = new SearchSourceBuilder();
            genSearchSourceBuilder(builder, command);
            builder.trackTotalHits(true);
            builder.from(0);
            builder.size(0);
            SearchRequest request = new SearchRequest(new String[] {config.getIndex()}, builder);
            SearchResponse response = connectorMapper.getConnection().search(request, RequestOptions.DEFAULT);
            return response.getHits().getTotalHits().value;
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e);
        }
    }

    @Override
    public Result reader(ESConnectorMapper connectorMapper, ReaderConfig config) {
        ESConfig cfg = connectorMapper.getConfig();
        SearchSourceBuilder builder = new SearchSourceBuilder();
        genSearchSourceBuilder(builder, config.getCommand());
        builder.from((config.getPageIndex() - 1) * config.getPageSize());
        builder.size(config.getPageSize());
        builder.timeout(TimeValue.timeValueMillis(10));

        try {
            SearchRequest rq = new SearchRequest(new String[] {cfg.getIndex()}, builder);
            SearchResponse searchResponse = connectorMapper.getConnection().search(rq, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            List<Map<String, Object>> list = new ArrayList<>();
            for (SearchHit hit : searchHits) {
                list.add(hit.getSourceAsMap());
            }
            return new Result(list);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e.getMessage());
        }
    }

    @Override
    public Result writer(ESConnectorMapper connectorMapper, WriterBatchConfig config) {
        List<Map> data = config.getData();
        if (CollectionUtils.isEmpty(data) || CollectionUtils.isEmpty(config.getFields())) {
            logger.error("writer data can not be empty.");
            throw new ConnectorException("writer data can not be empty.");
        }

        final Result result = new Result();
        final ESConfig cfg = connectorMapper.getConfig();
        final Field pkField = getPrimaryKeyField(config.getFields());
        final String primaryKeyName = pkField.getName();
        try {
            BulkRequest request = new BulkRequest();
            data.forEach(row -> addRequest(request, cfg.getIndex(), cfg.getType(), config.getEvent(), String.valueOf(row.get(primaryKeyName)), row));

            BulkResponse response = connectorMapper.getConnection().bulk(request, RequestOptions.DEFAULT);
            RestStatus restStatus = response.status();
            if (restStatus.getStatus() != RestStatus.OK.getStatus()) {
                throw new ConnectorException(String.format("error code:%s", restStatus.getStatus()));
            }
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
        Map<String, String> command = new HashMap<>();
        // 查询字段
        Table table = commandConfig.getTable();
        List<Field> column = table.getColumn();
        if (!CollectionUtils.isEmpty(column)) {
            List<String> fieldNames = column.stream().map(c -> c.getName()).collect(Collectors.toList());
            command.put(ConnectorConstant.OPERTION_QUERY, StringUtil.join(fieldNames, ","));
        }

        // 过滤条件
        List<Filter> filter = commandConfig.getFilter();
        if (!CollectionUtils.isEmpty(filter)) {
            command.put(ConnectorConstant.OPERTION_QUERY_FILTER, JsonUtil.objToJson(filter));
        }
        return command;
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        Table table = commandConfig.getTable();
        if (!CollectionUtils.isEmpty(table.getColumn())) {
            getPrimaryKeyField(table.getColumn());
        }
        return Collections.EMPTY_MAP;
    }

    private void genSearchSourceBuilder(SearchSourceBuilder builder, Map<String, String> command) {
        // 查询字段
        String fieldNamesJson = command.get(ConnectorConstant.OPERTION_QUERY);
        if (!StringUtil.isBlank(fieldNamesJson)) {
            builder.fetchSource(StringUtil.split(fieldNamesJson, ","), null);
        }

        // 过滤条件
        String filterJson = command.get(ConnectorConstant.OPERTION_QUERY_FILTER);
        List<Filter> filters = null;
        if (!StringUtil.isBlank(filterJson)) {
            filters = JsonUtil.jsonToArray(filterJson, Filter.class);
        }
        if (CollectionUtils.isEmpty(filters)) {
            builder.query(QueryBuilders.matchAllQuery());
            return;
        }

        List<Filter> and = filters.stream().filter(f -> OperationEnum.isAnd(f.getOperation())).collect(Collectors.toList());
        List<Filter> or = filters.stream().filter(f -> OperationEnum.isOr(f.getOperation())).collect(Collectors.toList());
        // where (id = 1 and name = 'tom') or id = 2 or id = 3
        BoolQueryBuilder q = QueryBuilders.boolQuery();
        if (!CollectionUtils.isEmpty(and) && !CollectionUtils.isEmpty(or)) {
            BoolQueryBuilder andQuery = QueryBuilders.boolQuery();
            and.forEach(f -> addFilter(andQuery, f));
            q.should(andQuery);
            genShouldQuery(q, or);
            builder.query(q);
            return;
        }

        if (!CollectionUtils.isEmpty(or)) {
            genShouldQuery(q, or);
            builder.query(q);
            return;
        }

        and.forEach(f -> addFilter(q, f));
        builder.query(q);
    }

    private void genShouldQuery(BoolQueryBuilder q, List<Filter> or) {
        or.forEach(f -> {
            BoolQueryBuilder orQuery = QueryBuilders.boolQuery();
            addFilter(orQuery, f);
            q.should(orQuery);
        });
    }

    private void addFilter(BoolQueryBuilder builder, Filter f) {
        if (filters.containsKey(f.getFilter())) {
            filters.get(f.getFilter()).apply(builder, f.getName(), f.getValue());
        }
    }

    private void addRequest(BulkRequest request, String index, String type, String event, String id, Map data) {
        if (isUpdate(event)) {
            UpdateRequest req = new UpdateRequest(index, type, id);
            req.doc(data, XContentType.JSON);
            request.add(req);
            return;
        }
        if (isInsert(event)) {
            IndexRequest req = new IndexRequest(index, type, id);
            req.source(data, XContentType.JSON);
            request.add(req);
            return;
        }
        if (isDelete(event)) {
            request.add(new DeleteRequest(index, type, id));
        }
    }

    private interface FilterMapper {
        void apply(BoolQueryBuilder builder, String key, String value);
    }
}