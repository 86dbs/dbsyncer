package org.dbsyncer.storage.support;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.storage.AbstractStorageService;
import org.dbsyncer.storage.StorageException;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.lucene.Shard;
import org.dbsyncer.storage.query.Option;
import org.dbsyncer.storage.query.Param;
import org.dbsyncer.storage.query.Query;
import org.dbsyncer.storage.util.DocumentUtil;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 将数据存储在磁盘，基于lucene实现
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/10 23:22
 */
public class DiskStorageServiceImpl extends AbstractStorageService {

    private Map<String, Shard> shards = new ConcurrentHashMap();

    /**
     * 相对路径/data/
     */
    private static final String PATH = new StringBuilder(System.getProperty("user.dir")).append(File.separatorChar).append("data")
            .append(File.separatorChar).toString();

    @PostConstruct
    private void init() {
        // 创建配置和日志索引shard
        getShard(getSharding(StorageEnum.CONFIG, null));
        getShard(getSharding(StorageEnum.LOG, null));
        getShard(getSharding(StorageEnum.BINLOG, null));
    }

    @Override
    protected Paging select(String sharding, Query query) {
        try {
            Shard shard = getShard(sharding);
            int pageNum = query.getPageNum() <= 0 ? 1 : query.getPageNum();
            int pageSize = query.getPageSize() <= 0 ? 20 : query.getPageSize();
            boolean queryTotal = query.isQueryTotal();
            // 根据修改时间 > 创建时间排序
            Sort sort = new Sort(new SortField(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, SortField.Type.LONG, true),
                    new SortField(ConfigConstant.CONFIG_MODEL_CREATE_TIME, SortField.Type.LONG, true));
            // 设置参数
            List<Param> params = query.getParams();
            if (!CollectionUtils.isEmpty(params)) {
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                params.forEach(p -> {
                    if (p.isNumber()) {
                        builder.add(IntPoint.newSetQuery(p.getKey(), NumberUtil.toInt(p.getValue())), BooleanClause.Occur.MUST);
                    } else {
                        builder.add(new TermQuery(new Term(p.getKey(), p.getValue())), BooleanClause.Occur.MUST);
                    }
                });
                BooleanQuery q = builder.build();
                return shard.query(new Option(q, queryTotal, params), pageNum, pageSize, sort);
            }

            return shard.query(new Option(new MatchAllDocsQuery(), queryTotal, null), pageNum, pageSize, sort);
        } catch (IOException e) {
            throw new StorageException(e);
        }
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
        try {
            shard.deleteBatch(terms);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void destroy() throws Exception {
        for (Map.Entry<String, Shard> m : shards.entrySet()) {
            m.getValue().close();
        }
        shards.clear();
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
                case BINLOG:
                    docs.add(DocumentUtil.convertBinlog2Doc(r));
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