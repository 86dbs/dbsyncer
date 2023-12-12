/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.storage.impl;

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
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.AbstractFilter;
import org.dbsyncer.sdk.filter.BooleanFilter;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.storage.AbstractStorageService;
import org.dbsyncer.storage.StorageException;
import org.dbsyncer.storage.lucene.Option;
import org.dbsyncer.storage.lucene.Shard;
import org.dbsyncer.storage.util.DocumentUtil;

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
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-09-10 23:22
 */
public class DiskStorageService extends AbstractStorageService {

    private Map<String, Shard> shards = new ConcurrentHashMap();

    /**
     * 相对路径/data/
     */
    private static final String PATH = new StringBuilder(System.getProperty("user.dir")).append(File.separatorChar).append("data")
            .append(File.separatorChar).toString();

    @Override
    public void init(Properties properties) {
        // 创建配置和日志索引shard
        getShard(getSharding(StorageEnum.CONFIG, null));
        getShard(getSharding(StorageEnum.LOG, null));
    }

    @Override
    protected Paging select(String sharding, Query query) {
        try {
            Shard shard = getShard(sharding);
            int pageNum = query.getPageNum() <= 0 ? 1 : query.getPageNum();
            int pageSize = query.getPageSize() <= 0 ? 20 : query.getPageSize();
            boolean desc = query.getSort().isDesc();
            // 根据修改时间 > 创建时间排序
            Sort sort = new Sort(new SortField(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, SortField.Type.LONG, desc),
                    new SortField(ConfigConstant.CONFIG_MODEL_CREATE_TIME, SortField.Type.LONG, desc));
            Option option = new Option();
            option.setQueryTotal(query.isQueryTotal());
            option.setFieldResolverMap(query.getFieldResolverMap());
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
        shards.computeIfPresent(sharding, (k, v) -> {
            v.deleteAll();
            return v;
        });
        shards.remove(sharding);
    }

    @Override
    protected void batchInsert(StorageEnum type, String sharding, List<Map> list) {
        batchExecute(type, sharding, list, (shard, docs) -> shard.insertBatch(docs));
    }

    @Override
    protected void batchUpdate(StorageEnum type, String sharding, List<Map> list) {
        batchExecute(type, sharding, list, (shard, docs) -> {
            for (Document doc : docs) {
                shard.update(getPrimaryKeyTerm(doc), doc);
            }
        });
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
        for (Map.Entry<String, Shard> m : shards.entrySet()) {
            m.getValue().close();
        }
        shards.clear();
    }

    private BooleanQuery buildQuery(List<AbstractFilter> filters, List<BooleanFilter> clauses, Set<String> highLightKeys) {
        if (!CollectionUtils.isEmpty(filters)) {
            return buildQueryWithFilters(filters, highLightKeys);
        }
        return buildQueryWithBooleanFilters(clauses, highLightKeys);
    }

    private BooleanQuery buildQueryWithFilters(List<AbstractFilter> filters, Set<String> highLightKeys) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        filters.forEach(p -> {
            FilterEnum filterEnum = FilterEnum.getFilterEnum(p.getFilter());
            BooleanClause.Occur occur = getOccur(p.getOperation());
            switch (filterEnum) {
                case EQUAL:
                case LIKE:
                    builder.add(DiskQueryHelper.newEqual(p), occur);
                    break;
                case LT:
                    builder.add(DiskQueryHelper.newLessThan(p), occur);
                    break;
            }

            if (p.isEnableHighLightSearch()) {
                highLightKeys.add(p.getName());
            }
        });
        return builder.build();
    }

    private BooleanQuery buildQueryWithBooleanFilters(List<BooleanFilter> clauses, Set<String> highLightKeys) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        clauses.forEach(c -> {
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
        list.forEach(r -> {
            switch (type) {
                case DATA:
                    docs.add(DocumentUtil.convertData2Doc(r));
                    break;
                case LOG:
                    docs.add(DocumentUtil.convertLog2Doc(r));
                    break;
                case CONFIG:
                    docs.add(DocumentUtil.convertConfig2Doc(r));
                    break;
                default:
                    break;
            }
        });
        try {
            mapper.apply(shard, docs);
        } catch (IOException e) {
            throw new StorageException(e);
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
        return shards.computeIfAbsent(sharding, k -> new Shard(PATH + k));
    }

    interface ExecuteMapper {
        void apply(Shard shard, List<Document> docs) throws IOException;
    }

}