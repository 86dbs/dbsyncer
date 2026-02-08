/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.elasticsearch.api.EasyVersion;
import org.dbsyncer.connector.elasticsearch.api.bulk.BulkItemResponse;
import org.dbsyncer.connector.elasticsearch.api.bulk.BulkResponse;
import org.dbsyncer.connector.elasticsearch.cdc.ESQuartzListener;
import org.dbsyncer.connector.elasticsearch.config.ESConfig;
import org.dbsyncer.connector.elasticsearch.enums.ESFieldTypeEnum;
import org.dbsyncer.connector.elasticsearch.schema.ElasticsearchSchemaResolver;
import org.dbsyncer.connector.elasticsearch.util.ESUtil;
import org.dbsyncer.connector.elasticsearch.validator.ESConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.connector.AbstractConnector;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Filter;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

    public static final String _SOURCE_INDEX = "_source_index";
    private final String _TARGET_INDEX = "_target_index";
    public static final String _TYPE = "_type";
    private final Map<String, FilterMapper> filters = new ConcurrentHashMap<>();
    private final ESConfigValidator configValidator = new ESConfigValidator();
    private final ElasticsearchSchemaResolver schemaResolver = new ElasticsearchSchemaResolver();

    public ElasticsearchConnector() {
        filters.putIfAbsent(FilterEnum.EQUAL.getName(), (builder, k, v)->builder.must(QueryBuilders.matchQuery(k, v)));
        filters.putIfAbsent(FilterEnum.NOT_EQUAL.getName(), (builder, k, v)->builder.mustNot(QueryBuilders.matchQuery(k, v)));
        filters.putIfAbsent(FilterEnum.GT.getName(), (builder, k, v)->builder.filter(QueryBuilders.rangeQuery(k).gt(v)));
        filters.putIfAbsent(FilterEnum.LT.getName(), (builder, k, v)->builder.filter(QueryBuilders.rangeQuery(k).lt(v)));
        filters.putIfAbsent(FilterEnum.GT_AND_EQUAL.getName(), (builder, k, v)->builder.filter(QueryBuilders.rangeQuery(k).gte(v)));
        filters.putIfAbsent(FilterEnum.LT_AND_EQUAL.getName(), (builder, k, v)->builder.filter(QueryBuilders.rangeQuery(k).lte(v)));
        filters.putIfAbsent(FilterEnum.LIKE.getName(), (builder, k, v)->builder.filter(QueryBuilders.wildcardQuery(k, v)));
    }

    @Override
    public String getConnectorType() {
        return "Elasticsearch";
    }

    @Override
    public TableTypeEnum getExtendedTableType() {
        return TableTypeEnum.SEMI;
    }

    @Override
    public Class<ESConfig> getConfigClass() {
        return ESConfig.class;
    }

    @Override
    public ConnectorInstance connect(ESConfig config, ConnectorServiceContext context) {
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
    public List<String> getDatabases(ESConnectorInstance connectorInstance) {
        try {
            RestHighLevelClient client = connectorInstance.getConnection();
            ClusterHealthRequest request = new ClusterHealthRequest();
            ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
            String clusterName = response.getClusterName();
            return Collections.singletonList(clusterName);
        } catch (IOException e) {
            logger.error("获取ES集群名称失败: {}", e.getMessage());
            throw new ElasticsearchException("获取ES集群名称失败: " + e.getMessage(), e);
        }
    }

    @Override
    public List<Table> getTable(ESConnectorInstance connectorInstance, ConnectorServiceContext context) {
        try {
            IndicesClient indices = connectorInstance.getConnection().indices();
            GetAliasesRequest aliasesRequest = new GetAliasesRequest();
            GetAliasesResponse indicesAlias = indices.getAlias(aliasesRequest, RequestOptions.DEFAULT);
            Map<String, Set<AliasMetadata>> aliases = indicesAlias.getAliases();
            if (!CollectionUtils.isEmpty(aliases)) {
                // 排除系统索引
                return aliases.keySet().stream().filter(index->!StringUtil.startsWith(index, StringUtil.POINT)).map(name-> {
                    Table table = new Table();
                    table.setName(name);
                    table.setType(TableTypeEnum.TABLE.getCode());
                    return table;
                }).collect(Collectors.toList());
            }
            return new ArrayList<>();
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ElasticsearchException(e);
        }
    }

    @Override
    public List<MetaInfo> getMetaInfo(ESConnectorInstance connectorInstance, ConnectorServiceContext context) {
        List<MetaInfo> metaInfos = new ArrayList<>();
        try {
            for (Table table : context.getTablePatterns()) {
                // 自定义SQL
                if (TableTypeEnum.getTableType(table.getType()) == getExtendedTableType()) {
                    getExtendedMetaInfo(connectorInstance, metaInfos, table);
                    continue;
                }
                String index = table.getName();
                GetIndexRequest request = new GetIndexRequest(index);
                GetIndexResponse indexResponse = connectorInstance.getConnection().indices().get(request, RequestOptions.DEFAULT);
                MappingMetadata mappingMetaData = indexResponse.getMappings().get(index);
                List<Field> fields = new ArrayList<>();
                // 低于7.x 版本
                if (EasyVersion.V_7_0_0.after(connectorInstance.getVersion())) {
                    Map<String, Object> sourceMap = XContentHelper.convertToMap(mappingMetaData.source().compressedReference(), true, null).v2();
                    if (CollectionUtils.isEmpty(sourceMap)) {
                        throw new ElasticsearchException("未获取到索引配置");
                    }
                    Iterator<String> iterator = sourceMap.keySet().iterator();
                    String indexType = null;
                    if (iterator.hasNext()) {
                        indexType = iterator.next();
                        parseProperties(fields, (Map) sourceMap.get(indexType));
                    }
                    if (StringUtil.isBlank(indexType)) {
                        throw new ElasticsearchException("索引type为空");
                    }
                    metaInfos.add(buildMetaInfo(table.getType(), index, fields, indexType));
                    continue;
                }

                // 7.x 版本以上
                parseProperties(fields, mappingMetaData.sourceAsMap());
                metaInfos.add(buildMetaInfo(table.getType(), index, fields, null));
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ElasticsearchException(e);
        }
        return metaInfos;
    }

    private void getExtendedMetaInfo(ESConnectorInstance connectorInstance, List<MetaInfo> metaInfos, Table table) {
        MetaInfo metaInfo = new MetaInfo();
        metaInfo.setTable(table.getName());
        metaInfo.setTableType(table.getType());
        metaInfo.setColumn(table.getColumn());
        Properties extInfo = metaInfo.getExtInfo();
        extInfo.putAll(table.getExtInfo());
        // 低于7.x 版本
        if (EasyVersion.V_7_0_0.after(connectorInstance.getVersion())) {
            extInfo.put(_TYPE, null);
        } else {
            // 设置默认值
            extInfo.setProperty(_TYPE, extInfo.getProperty(_TYPE, "_doc"));
        }
        metaInfos.add(metaInfo);
    }

    private MetaInfo buildMetaInfo(String tableType, String index, List<Field> fields, String indexType) {
        MetaInfo metaInfo = new MetaInfo();
        metaInfo.setTable(index);
        metaInfo.setTableType(tableType);
        metaInfo.setColumn(fields);
        if (StringUtil.isNotBlank(indexType)) {
            metaInfo.getExtInfo().put(_TYPE, indexType);
        }
        return metaInfo;
    }

    @Override
    public long getCount(ESConnectorInstance connectorInstance, Map<String, String> command) {
        try {
            SearchSourceBuilder builder = new SearchSourceBuilder();
            genSearchSourceBuilder(builder, command);
            // 7.x 版本以上
            if (EasyVersion.V_7_0_0.onOrBefore(connectorInstance.getVersion())) {
                builder.trackTotalHits(true);
            }
            builder.from(0);
            builder.size(0);
            SearchRequest request = new SearchRequest(new String[]{command.get(_SOURCE_INDEX)}, builder);
            SearchResponse response = connectorInstance.getConnection().searchWithVersion(request, RequestOptions.DEFAULT);
            return response.getHits().getTotalHits().value;
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ElasticsearchException(e);
        }
    }

    @Override
    public Result reader(ESConnectorInstance connectorInstance, ReaderContext context) {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        genSearchSourceBuilder(builder, context.getCommand());
        builder.timeout(TimeValue.timeValueSeconds(connectorInstance.getConfig().getTimeoutSeconds()));
        List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(context.getSourceTable());
        primaryKeys.forEach(pk->builder.sort(pk, SortOrder.ASC));
        // 深度分页
        if (!CollectionUtils.isEmpty(context.getCursors())) {
            builder.from(0);
            builder.searchAfter(context.getCursors());
        } else {
            builder.from((context.getPageIndex() - 1) * context.getPageSize());
        }
        builder.size(Math.min(context.getPageSize(), 10000));

        try {
            SearchRequest rq = new SearchRequest(new String[]{context.getCommand().get(_SOURCE_INDEX)}, builder);
            SearchResponse searchResponse = connectorInstance.getConnection().searchWithVersion(rq, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            List<Map<String, Object>> list = new ArrayList<>();
            for (SearchHit hit : searchHits) {
                list.add(hit.getSourceAsMap());
            }
            if (searchResponse.getInternalResponse().timedOut()) {
                throw new ElasticsearchException("search timeout:" + searchResponse.getTook().getMillis() + "ms, pageIndex:" + context.getPageIndex() + ", pageSize:" + context.getPageSize());
            }
            return new Result(list);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ElasticsearchException(e.getMessage());
        }
    }

    @Override
    public Result writer(ESConnectorInstance connectorInstance, PluginContext context) {
        List<Map> data = context.getTargetList();
        if (CollectionUtils.isEmpty(data)) {
            logger.error("writer data can not be empty.");
            throw new ElasticsearchException("writer data can not be empty.");
        }

        Result result = new Result();
        final List<Field> pkFields = PrimaryKeyUtil.findExistPrimaryKeyFields(context.getTargetFields());
        try {
            final BulkRequest request = new BulkRequest();
            final String pk = pkFields.get(0).getName();
            final String indexName = context.getCommand().get(_TARGET_INDEX);
            final String type = context.getCommand().get(_TYPE);
            data.forEach(row->addRequest(request, indexName, type, context.getEvent(), String.valueOf(row.get(pk)), row));

            BulkResponse response = connectorInstance.getConnection().bulkWithVersion(request, RequestOptions.DEFAULT);
            RestStatus restStatus = response.status();
            if (restStatus.getStatus() != RestStatus.OK.getStatus()) {
                throw new ElasticsearchException(String.format("error code:%s", restStatus.getStatus()));
            }
            BulkItemResponse[] items = response.getItems();
            BulkItemResponse r = null;
            for (int i = 0; i < items.length; i++) {
                r = items[i];
                if (r.isFailed()) {
                    result.getFailData().add(data.get(i));
                    result.getError().append("\n[").append(i).append("]: index [").append(r.getIndex()).append("], type [").append(r.getType()).append("], id [").append(r.getId())
                            .append("], message [").append(r.getFailureMessage()).append("]");
                    continue;
                }
                result.getSuccessData().add(data.get(i));
            }
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
        command.put(_SOURCE_INDEX, table.getName());
        List<Field> column = table.getColumn();
        if (!CollectionUtils.isEmpty(column)) {
            List<String> fieldNames = column.stream().map(c->c.getName()).collect(Collectors.toList());
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
        PrimaryKeyUtil.findTablePrimaryKeys(table);
        Map<String, String> command = new HashMap<>();
        command.put(_TARGET_INDEX, table.getName());
        command.put(_TYPE, String.valueOf(table.getExtInfo().get(_TYPE)));
        return command;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new ESQuartzListener();
        }
        return null;
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    private void parseProperties(List<Field> fields, Map<String, Object> sourceMap) {
        if (CollectionUtils.isEmpty(sourceMap)) {
            throw new ElasticsearchException("未获取到索引字段.");
        }
        Map<String, Object> properties = (Map<String, Object>) sourceMap.get(ESUtil.PROPERTIES);
        if (CollectionUtils.isEmpty(properties)) {
            throw new ElasticsearchException("查询字段不能为空.");
        }
        properties.forEach((fieldName, c)-> {
            Map fieldDesc = (Map) c;
            String columnType = (String) fieldDesc.get("type");
            // dynamic => object
            if (columnType == null) {
                columnType = ESFieldTypeEnum.OBJECT.getCode();
            }

            // 如果时间类型做了format, 按字符串类型处理
            if (StringUtil.equals(ESFieldTypeEnum.DATE.getCode(), columnType)) {
                Object format = fieldDesc.get("format");
                if (format != null) {
                    Field dateField = new Field(fieldName, columnType, ESFieldTypeEnum.KEYWORD.getType());
                    dateField.getExtInfo().put("format", format);
                    fields.add(dateField);
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

        List<Filter> and = filters.stream().filter(f->OperationEnum.isAnd(f.getOperation())).collect(Collectors.toList());
        List<Filter> or = filters.stream().filter(f->OperationEnum.isOr(f.getOperation())).collect(Collectors.toList());
        // where (id = 1 and name = 'tom') or id = 2 or id = 3
        BoolQueryBuilder q = QueryBuilders.boolQuery();
        if (!CollectionUtils.isEmpty(and) && !CollectionUtils.isEmpty(or)) {
            BoolQueryBuilder andQuery = QueryBuilders.boolQuery();
            and.forEach(f->addFilter(andQuery, f));
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

        and.forEach(f->addFilter(q, f));
        builder.query(q);
    }

    private void genShouldQuery(BoolQueryBuilder q, List<Filter> or) {
        or.forEach(f-> {
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