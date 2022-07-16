package org.dbsyncer.storage.support;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.IndexableField;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 将数据存储在磁盘，基于lucene实现
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/10 23:22
 */
public class DiskStorageServiceImpl extends AbstractStorageService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, Shard> map = new ConcurrentHashMap();

    /**
     * 相对路径/data/
     */
    private static final String PATH = new StringBuilder(System.getProperty("user.dir")).append(File.separatorChar).append("data")
            .append(File.separatorChar).toString();

    @PostConstruct
    private void init() {
        try {
            // 创建配置和日志索引shard
            String config = StorageEnum.CONFIG.getType();
            map.putIfAbsent(config, new Shard(PATH + config));

            String log = StorageEnum.LOG.getType();
            map.putIfAbsent(log, new Shard(PATH + log));
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public Paging select(Query query) throws IOException {
        Shard shard = map.get(query.getCollection());

        // 检查是否存在历史
        if (null == shard) {
            shard = cacheShardIfExist(query.getCollection());
        }

        if (null != shard) {
            int pageNum = query.getPageNum() <= 0 ? 1 : query.getPageNum();
            int pageSize = query.getPageSize() <= 0 ? 20 : query.getPageSize();
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
                return shard.query(new Option(q, params), pageNum, pageSize, sort);
            }

            return shard.query(new Option(new MatchAllDocsQuery()), pageNum, pageSize, sort);
        }
        return new Paging(query.getPageNum(), query.getPageSize());
    }

    @Override
    public void insert(StorageEnum type, String collection, Map params) throws IOException {
        createShardIfNotExist(collection);
        Document doc = DocumentUtil.convertConfig2Doc(params);
        map.get(collection).insert(doc);
    }

    @Override
    public void update(StorageEnum type, String collection, Map params) throws IOException {
        createShardIfNotExist(collection);
        Document doc = DocumentUtil.convertConfig2Doc(params);
        IndexableField field = doc.getField(ConfigConstant.CONFIG_MODEL_ID);
        map.get(collection).update(new Term(ConfigConstant.CONFIG_MODEL_ID, field.stringValue()), doc);
    }

    @Override
    public void delete(StorageEnum type, String collection, String id) throws IOException {
        createShardIfNotExist(collection);
        map.get(collection).delete(new Term(ConfigConstant.CONFIG_MODEL_ID, id));
    }

    @Override
    public void deleteAll(StorageEnum type, String collection) throws IOException {
        synchronized (this) {
            Shard shard = map.get(collection);
            if (null != shard) {
                shard.deleteAll();
                map.remove(collection);
            }
        }
    }

    @Override
    public void insertLog(StorageEnum type, String collection, Map<String, Object> params) throws IOException {
        createShardIfNotExist(collection);
        Document doc = DocumentUtil.convertLog2Doc(params);
        map.get(collection).insert(doc);
    }

    @Override
    public void insertData(StorageEnum type, String collection, List<Map> list) throws IOException {
        createShardIfNotExist(collection);
        List<Document> docs = list.stream().map(r -> DocumentUtil.convertData2Doc(r)).collect(Collectors.toList());
        map.get(collection).insertBatch(docs);
    }

    /**
     * 如果不存在分片则创建(线程安全)
     * <p>/data/config</p>
     * <p>/data/log</p>
     * <p>/data/data/123</p>
     *
     * @param collectionId
     * @throws IOException
     */
    private void createShardIfNotExist(String collectionId) throws IOException {
        synchronized (this) {
            if (null == map.get(collectionId)) {
                map.putIfAbsent(collectionId, new Shard(PATH + collectionId));
            }
        }
    }

    private Shard cacheShardIfExist(String collectionId) {
        String path = PATH + collectionId;
        if (new File(path).exists()) {
            try {
                map.putIfAbsent(collectionId, new Shard(path));
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        return map.get(collectionId);
    }

    @Override
    public void destroy() throws Exception {
        for (Map.Entry<String, Shard> m : map.entrySet()) {
            m.getValue().close();
        }
        map.clear();
    }
}