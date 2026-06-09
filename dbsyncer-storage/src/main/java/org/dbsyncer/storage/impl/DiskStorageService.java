/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.storage.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.enums.SortEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.AbstractFilter;
import org.dbsyncer.sdk.filter.BooleanFilter;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.storage.AbstractStorageService;
import org.dbsyncer.storage.StorageException;
import org.dbsyncer.storage.lucene.Option;
import org.dbsyncer.storage.lucene.Shard;
import org.dbsyncer.storage.util.DocumentUtil;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 将数据存储在磁盘，基于lucene实现
 *
 * @author AE86
 * @version 1.0.0
 * @date 2023-09-10 23:22
 */
public class DiskStorageService extends AbstractStorageService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String COMMIT_TASK_KEY = "diskStorageLuceneCommit";

    private final long DEFAULT_COMMIT_INTERVAL_MS = 3000L;

    private final ScheduledTaskService scheduledTaskService;

    private final Map<String, Shard> shards = new ConcurrentHashMap<>();

    private long commitIntervalMs = DEFAULT_COMMIT_INTERVAL_MS;

    /**
     * 相对路径/data/
     */
    private static final String PATH = System.getProperty("user.dir") + File.separatorChar + "data" + File.separatorChar;

    public DiskStorageService(ScheduledTaskService scheduledTaskService) {
        this.scheduledTaskService = scheduledTaskService;
    }

    @Override
    public void init(Properties properties) {
        if (properties != null) {
            commitIntervalMs = NumberUtil.toLong(properties.getProperty("dbsyncer.storage.disk.commit-interval-ms"), DEFAULT_COMMIT_INTERVAL_MS);
            if (commitIntervalMs < 500L) {
                commitIntervalMs = DEFAULT_COMMIT_INTERVAL_MS;
            }
        }
        // 创建配置和日志索引shard
        getShard(getSharding(StorageEnum.CONFIG, null));
        getShard(getSharding(StorageEnum.LOG, null));
        startCommitScheduler();
    }

    @Override
    protected Paging select(String sharding, Query query) {
        try {
            Shard shard = getShard(sharding);
            int pageNum = query.getPageNum() <= 0 ? 1 : query.getPageNum();
            int pageSize = query.getPageSize() <= 0 ? 20 : query.getPageSize();
            boolean desc = query.getSort().isDesc();
            Sort sort = buildDiskSort(query, desc);

            // 自定义
            Option option = new Option();
            option.setQueryTotal(query.isQueryTotal());
            option.setFieldResolverMap(query.getFieldResolverMap());
            if (query.hasSelectField()) {
                option.setSelectFields(query.getSelectFlied());
            }
            // 设置参数
            BooleanFilter baseQuery = query.getBooleanFilter();
            List<AbstractFilter> filters = baseQuery.getFilters();
            List<BooleanFilter> clauses = baseQuery.getClauses();
            if (CollectionUtils.isEmpty(clauses) && CollectionUtils.isEmpty(filters)) {
                option.setQuery(new MatchAllDocsQuery());
                return shard.query(option, pageNum, pageSize, sort);
            }

            Set<String> highLightKeys = new HashSet<>();
            BooleanQuery build = buildQuery(filters, clauses, highLightKeys);
            option.setQuery(build);

            // 高亮查询
            if (!CollectionUtils.isEmpty(highLightKeys)) {
                option.setHighLightKeys(highLightKeys);
                option.setEnableHighLightSearch(true);
                SimpleHTMLFormatter formatter = new SimpleHTMLFormatter("<span style='color:red'>", "</span>");
                option.setHighlighter(new Highlighter(formatter, new QueryScorer(build)));
            }

            return shard.query(option, pageNum, pageSize, sort);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    protected void delete(String sharding, Query query) {
        Shard shard = getShard(sharding);
        BooleanFilter q = query.getBooleanFilter();
        shard.delete(buildQuery(q.getFilters(), q.getClauses(), new HashSet<>()));
    }

    @Override
    public void deleteAll(String sharding) {
        shards.computeIfPresent(sharding, (k, v)-> {
            v.deleteAll();
            return v;
        });
        shards.remove(sharding);
    }

    @Override
    protected void batchInsert(StorageEnum type, String sharding, List<Map> list) {
        batchExecute(type, sharding, list, Shard::insertBatch);
    }

    @Override
    protected void batchUpdate(StorageEnum type, String sharding, List<Map> list) {
        if (CollectionUtils.isEmpty(list)) {
            return;
        }
        Shard shard = getShard(sharding);
        List<Document> docs = new ArrayList<>(list.size());
        List<Term> terms = new ArrayList<>(list.size());
        list.forEach(r -> {
            Document doc = convertDocument(type, r);
            if (doc != null) {
                docs.add(doc);
                terms.add(getPrimaryKeyTerm(doc));
            }
        });
        if (!docs.isEmpty()) {
            shard.updateBatch(terms, docs);
        }
    }

    @Override
    protected void batchDelete(StorageEnum type, String sharding, List<String> ids) {
        Shard shard = getShard(sharding);
        int size = ids.size();
        Term[] terms = new Term[size];
        for (int i = 0; i < size; i++) {
            terms[i] = getPrimaryKeyTerm(ids.get(i));
        }
        shard.deleteBatch(terms);
    }

    @Override
    public void destroy() throws Exception {
        stopCommitScheduler();
        commitAllShards();
        for (Map.Entry<String, Shard> m : shards.entrySet()) {
            m.getValue().close();
        }
        shards.clear();
    }

    /**
     * 定时提交所有分片，合并高频写入（如同步 DATA）产生的 segment，降低 Reader 堆外占用。
     */
    private void startCommitScheduler() {
        if (scheduledTaskService == null) {
            return;
        }
        scheduledTaskService.start(COMMIT_TASK_KEY, commitIntervalMs, this::commitAllShards);
        logger.info("DiskStorage Lucene 定时 commit 已启动, interval={}ms", commitIntervalMs);
    }

    private void stopCommitScheduler() {
        if (scheduledTaskService != null) {
            scheduledTaskService.stop(COMMIT_TASK_KEY);
        }
    }

    private void commitAllShards() {
        for (Map.Entry<String, Shard> entry : shards.entrySet()) {
            Shard shard = entry.getValue();
            if (!shard.isDirty()) {
                continue;
            }
            try {
                if (shard.commitIfDirty()) {
                    logger.debug("Lucene shard committed: {}", entry.getKey());
                }
            } catch (IOException e) {
                logger.warn("Lucene shard commit failed: {}", entry.getKey(), e);
            }
        }
    }

    private BooleanQuery buildQuery(List<AbstractFilter> filters, List<BooleanFilter> clauses, Set<String> highLightKeys) {
        if (!CollectionUtils.isEmpty(filters)) {
            return buildQueryWithFilters(filters, highLightKeys);
        }
        return buildQueryWithBooleanFilters(clauses, highLightKeys);
    }

    private BooleanQuery buildQueryWithFilters(List<AbstractFilter> filters, Set<String> highLightKeys) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        filters.forEach(p-> {
            FilterEnum filterEnum = FilterEnum.getFilterEnum(p.getFilter());
            BooleanClause.Occur occur = getOccur(p.getOperation());
            switch (filterEnum) {
                case EQUAL:
                case LIKE:
                    builder.add(DiskQueryHelper.newEqual(p), occur);
                    break;
                case LT:
                    builder.add(DiskQueryHelper.newLessThan(p, FilterEnum.LT), occur);
                    break;
                case LT_AND_EQUAL:
                    builder.add(DiskQueryHelper.newLessThan(p, FilterEnum.LT_AND_EQUAL), occur);
                    break;
                case GT:
                    builder.add(DiskQueryHelper.newGreaterThan(p, FilterEnum.GT), occur);
                    break;
                case GT_AND_EQUAL:
                    builder.add(DiskQueryHelper.newGreaterThan(p, FilterEnum.GT_AND_EQUAL), occur);
                    break;
                default:
                    throw new StorageException("Unsupported filter type: " + filterEnum.getName());
            }

            if (p.isEnableHighLightSearch()) {
                highLightKeys.add(p.getName());
            }
        });
        return builder.build();
    }

    private BooleanQuery buildQueryWithBooleanFilters(List<BooleanFilter> clauses, Set<String> highLightKeys) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        clauses.forEach(c-> {
            if (!CollectionUtils.isEmpty(c.getFilters())) {
                BooleanQuery cBuild = buildQueryWithFilters(c.getFilters(), highLightKeys);
                builder.add(cBuild, getOccur(c.getOperationEnum().getName()));
            }
        });
        return builder.build();
    }

    private BooleanClause.Occur getOccur(String operation) {
        return OperationEnum.isAnd(operation) ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD;
    }

    private Term getPrimaryKeyTerm(Document doc) {
        return new Term(ConfigConstant.CONFIG_MODEL_ID, doc.getField(ConfigConstant.CONFIG_MODEL_ID).stringValue());
    }

    private Term getPrimaryKeyTerm(String id) {
        return new Term(ConfigConstant.CONFIG_MODEL_ID, id);
    }

    private void batchExecute(StorageEnum type, String sharding, List<Map> list, ExecuteMapper mapper) {
        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        Shard shard = getShard(sharding);
        List<Document> docs = new ArrayList<>();
        list.forEach(r-> {
            Document doc = convertDocument(type, r);
            if (doc != null) {
                docs.add(doc);
            }
        });
        try {
            mapper.apply(shard, docs);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    private Document convertDocument(StorageEnum type, Map r) {
        switch (type) {
            case DATA:
                return DocumentUtil.convertData2Doc(r);
            case LOG:
                return DocumentUtil.convertLog2Doc(r);
            case CONFIG:
                return DocumentUtil.convertConfig2Doc(r);
            case TASK:
                return DocumentUtil.convertTask2Doc(r);
            case VALIDATE_SYNC_DETAIL:
                return DocumentUtil.convertValidateSyncDetail2Doc(r);
            default:
                return null;
        }
    }

    /**
     * 如果不存在分片则创建(线程安全)
     * <p>/data/config</p>
     * <p>/data/log</p>
     * <p>/data/data/123</p>
     *
     * @param sharding
     * @throws IOException
     */
    private Shard getShard(String sharding) {
        return shards.computeIfAbsent(sharding, k->new Shard(PATH + k));
    }

    /**
     * Lucene 排序：与 {@link org.dbsyncer.connector.mysql.storage.MySQLStorageService} 语义对齐，
     * 自定义顺序优先；否则按 updateTime、createTime（与既有磁盘索引行为一致）。
     *
     * @param query   查询条件
     * @param defaultDesc 默认降序（无自定义排序字段时用于时间与默认分支）
     */
    private Sort buildDiskSort(Query query, boolean defaultDesc) {
        if (query.hasCustomOrderBy()) {
            List<Query.OrderBy> orderByList = query.getOrderByList();
            SortField[] sortFields = new SortField[orderByList.size()];
            for (int i = 0; i < orderByList.size(); i++) {
                Query.OrderBy orderBy = orderByList.get(i);
                SortEnum effectiveSort = orderBy.getSort() != null ? orderBy.getSort() : query.getSort();
                boolean reverse = effectiveSort.isDesc();
                String fieldName = orderBy.getFieldName();
                sortFields[i] = new SortField(fieldName, resolveSortFieldType(fieldName), reverse);
            }
            return new Sort(sortFields);
        }
        return new Sort(
                new SortField(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, SortField.Type.LONG, defaultDesc),
                new SortField(ConfigConstant.CONFIG_MODEL_CREATE_TIME, SortField.Type.LONG, defaultDesc));
    }

    /**
     * 索引字段类型（用于 {@link SortField}）。未单独建 DocValues 的字段排序可能运行时失败，需与 {@link org.dbsyncer.storage.util.DocumentUtil} 保持一致。
     */
    private SortField.Type resolveSortFieldType(String fieldName) {
        if (fieldName == null) {
            return SortField.Type.STRING;
        }
        switch (fieldName) {
            case ConfigConstant.CONFIG_MODEL_CREATE_TIME:
            case ConfigConstant.CONFIG_MODEL_UPDATE_TIME:
            case ConfigConstant.TASK_SOURCE_TOTAL:
            case ConfigConstant.TASK_TARGET_TOTAL:
            case ConfigConstant.TASK_DIFF_TOTAL:
            case ConfigConstant.TASK_FIXED_TOTAL:
                return SortField.Type.LONG;
            case ConfigConstant.DATA_SUCCESS:
            case ConfigConstant.TASK_STATUS:
                return SortField.Type.INT;
            default:
                return SortField.Type.STRING;
        }
    }

    interface ExecuteMapper {

        void apply(Shard shard, List<Document> docs) throws IOException;
    }

}