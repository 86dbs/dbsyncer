/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.elasticsearch.cdc.ESQuartzListener;
import org.dbsyncer.connector.elasticsearch.config.ESConfig;
import org.dbsyncer.connector.elasticsearch.enums.ESFieldTypeEnum;
import org.dbsyncer.connector.elasticsearch.schema.ESDateValueMapper;
import org.dbsyncer.connector.elasticsearch.schema.ESOtherValueMapper;
import org.dbsyncer.connector.elasticsearch.util.ESUtil;
import org.dbsyncer.connector.elasticsearch.validator.ESConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.ReaderConfig;
import org.dbsyncer.sdk.config.WriterBatchConfig;
import org.dbsyncer.sdk.connector.AbstractConnector;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Filter;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ES连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-11-25 23:10
 */
public final class ElasticsearchConnector extends AbstractConnector implements ConnectorService<ESConnectorInstance, ESConfig> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String TYPE = "Elasticsearch";

    private final Map<String, FilterMapper> filters = new LinkedHashMap<>();

    private final ESConfigValidator configValidator = new ESConfigValidator();

    @PostConstruct
    private void init() {
        VALUE_MAPPERS.put(Types.DATE, new ESDateValueMapper());
        VALUE_MAPPERS.put(Types.OTHER, new ESOtherValueMapper());

        filters.putIfAbsent(FilterEnum.EQUAL.getName(), (builder, k, v) -> builder.must(QueryBuilders.matchQuery(k, v)));
        filters.putIfAbsent(FilterEnum.NOT_EQUAL.getName(), (builder, k, v) -> builder.mustNot(QueryBuilders.matchQuery(k, v)));
        filters.putIfAbsent(FilterEnum.GT.getName(), (builder, k, v) -> builder.filter(QueryBuilders.rangeQuery(k).gt(v)));
        filters.putIfAbsent(FilterEnum.LT.getName(), (builder, k, v) -> builder.filter(QueryBuilders.rangeQuery(k).lt(v)));
        filters.putIfAbsent(FilterEnum.GT_AND_EQUAL.getName(), (builder, k, v) -> builder.filter(QueryBuilders.rangeQuery(k).gte(v)));
        filters.putIfAbsent(FilterEnum.LT_AND_EQUAL.getName(), (builder, k, v) -> builder.filter(QueryBuilders.rangeQuery(k).lte(v)));
        filters.putIfAbsent(FilterEnum.LIKE.getName(), (builder, k, v) -> builder.filter(QueryBuilders.wildcardQuery(k, v)));
    }

    @Override
    public String getConnectorType() {
        return TYPE;
    }

    @Override
    public boolean isSupportedTiming() {
        return true;
    }

    @Override
    public boolean isSupportedLog() {
        return false;
    }

    @Override
    public Class<ESConfig> getConfigClass() {
        return ESConfig.class;
    }

    @Override
    public ConnectorInstance connect(ESConfig config) {
        return new ESConnectorInstance(config);
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public void disconnect(ESConnectorInstance connectorInstance) {
        connectorInstance.close();
    }

    @Override
    public boolean isAlive(ESConnectorInstance connectorInstance) {
        try {
            RestHighLevelClient client = connectorInstance.getConnection();
            return client.ping(RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ElasticsearchException(e.getMessage());
        }
    }

    @Override
    public String getConnectorInstanceCacheKey(ESConfig config) {
        return String.format("%s-%s-%s-%s", config.getConnectorType(), config.getUrl(), config.getIndex(), config.getUsername());
    }

    @Override
    public List<Table> getTable(ESConnectorInstance connectorInstance) {
        try {
            ESConfig config = connectorInstance.getConfig();
            GetIndexRequest request = new GetIndexRequest(config.getIndex());
            GetIndexResponse indexResponse = connectorInstance.getConnection().indices().get(request, RequestOptions.DEFAULT);
            MappingMetadata mappingMetaData = indexResponse.getMappings().get(config.getIndex());
            List<Table> tables = new ArrayList<>();
            // 6.x 版本
            if (Version.V_7_0_0.after(connectorInstance.getVersion())) {
                Map<String, Object> sourceMap = XContentHelper.convertToMap(mappingMetaData.source().compressedReference(), true, null).v2();
                sourceMap.keySet().forEach(tableName -> tables.add(new Table(tableName)));
                return tables;
            }

            // 7.x 版本以上
            tables.add(new Table(mappingMetaData.type()));
            return tables;
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ElasticsearchException(e);
        }
    }

    @Override
    public MetaInfo getMetaInfo(ESConnectorInstance connectorInstance, String tableName) {
        List<Field> fields = new ArrayList<>();
        try {
            ESConfig config = connectorInstance.getConfig();
            GetIndexRequest request = new GetIndexRequest(config.getIndex());
            GetIndexResponse indexResponse = connectorInstance.getConnection().indices().get(request, RequestOptions.DEFAULT);
            MappingMetadata mappingMetaData = indexResponse.getMappings().get(config.getIndex());
            // 6.x 版本
            if (Version.V_7_0_0.after(connectorInstance.getVersion())) {
                Map<String, Object> sourceMap = XContentHelper.convertToMap(mappingMetaData.source().compressedReference(), true, null).v2();
                parseProperties(fields, (Map) sourceMap.get(tableName));
                return new MetaInfo().setColumn(fields);
            }

            // 7.x 版本以上
            parseProperties(fields, mappingMetaData.sourceAsMap());
            return new MetaInfo().setColumn(fields);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ElasticsearchException(e);
        }
    }

    @Override
    public long getCount(ESConnectorInstance connectorInstance, Map<String, String> command) {
        try {
            ESConfig config = connectorInstance.getConfig();
            SearchSourceBuilder builder = new SearchSourceBuilder();
            genSearchSourceBuilder(builder, command);
            // 7.x 版本以上
            if (Version.V_7_0_0.onOrBefore(connectorInstance.getVersion())) {
                builder.trackTotalHits(true);
            }
            builder.from(0);
            builder.size(0);
            SearchRequest request = new SearchRequest(new String[]{config.getIndex()}, builder);
            SearchResponse response = connectorInstance.getConnection().searchWithVersion(request, RequestOptions.DEFAULT);
            return response.getHits().getTotalHits().value;
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ElasticsearchException(e);
        }
    }

    @Override
    public Result reader(ESConnectorInstance connectorInstance, ReaderConfig config) {
        ESConfig cfg = connectorInstance.getConfig();
        SearchSourceBuilder builder = new SearchSourceBuilder();
        genSearchSourceBuilder(builder, config.getCommand());
        builder.from((config.getPageIndex() - 1) * config.getPageSize());
        builder.size(config.getPageSize());
        builder.timeout(TimeValue.timeValueMillis(10));
        List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(config.getTable());
        if (!CollectionUtils.isEmpty(primaryKeys)) {
            primaryKeys.forEach(pk -> builder.sort(pk, SortOrder.ASC));
            // 深度分页
            if (!CollectionUtils.isEmpty(config.getCursors())) {
                builder.from(0);
                builder.searchAfter(config.getCursors());
            }
        }

        try {
            SearchRequest rq = new SearchRequest(new String[]{cfg.getIndex()}, builder);
            SearchResponse searchResponse = connectorInstance.getConnection().searchWithVersion(rq, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            List<Map<String, Object>> list = new ArrayList<>();
            for (SearchHit hit : searchHits) {
                list.add(hit.getSourceAsMap());
            }
            return new Result(list);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ElasticsearchException(e.getMessage());
        }
    }

    @Override
    public Result writer(ESConnectorInstance connectorInstance, WriterBatchConfig config) {
        List<Map> data = config.getData();
        if (CollectionUtils.isEmpty(data) || CollectionUtils.isEmpty(config.getFields())) {
            logger.error("writer data can not be empty.");
            throw new ElasticsearchException("writer data can not be empty.");
        }

        final Result result = new Result();
        final ESConfig cfg = connectorInstance.getConfig();
        final List<Field> pkFields = PrimaryKeyUtil.findConfigPrimaryKeyFields(config);
        try {
            BulkRequest request = new BulkRequest();
            // 默认取第一个主键
            final String pk = pkFields.get(0).getName();
            data.forEach(row -> addRequest(request, cfg.getIndex(), config.getTableName(), config.getEvent(), String.valueOf(row.get(pk)), row));

            BulkResponse response = connectorInstance.getConnection().bulkWithVersion(request, RequestOptions.DEFAULT);
            RestStatus restStatus = response.status();
            if (restStatus.getStatus() != RestStatus.OK.getStatus()) {
                throw new ElasticsearchException(String.format("error code:%s", restStatus.getStatus()));
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
        PrimaryKeyUtil.findTablePrimaryKeys(commandConfig.getTable());
        return Collections.EMPTY_MAP;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new ESQuartzListener();
        }
        return null;
    }

    private void parseProperties(List<Field> fields, Map<String, Object> sourceMap) {
        Map<String, Object> properties = (Map<String, Object>) sourceMap.get(ESUtil.PROPERTIES);
        if (CollectionUtils.isEmpty(properties)) {
            throw new ElasticsearchException("查询字段不能为空.");
        }
        properties.forEach((fieldName, c) -> {
            Map fieldDesc = (Map) c;
            String columnType = (String) fieldDesc.get("type");
            // 如果时间类型做了format, 按字符串类型处理
            if (StringUtil.equals(ESFieldTypeEnum.DATE.getCode(), columnType)) {
                if (fieldDesc.containsKey("format")) {
                    fields.add(new Field(fieldName, columnType, ESFieldTypeEnum.KEYWORD.getType()));
                    return;
                }
            }
            fields.add(new Field(fieldName, columnType, ESFieldTypeEnum.getType(columnType)));
        });
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