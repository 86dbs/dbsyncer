package org.dbsyncer.storage.support;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.storage.AbstractStorageService;
import org.dbsyncer.storage.StorageException;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.lucene.Shard;
import org.dbsyncer.storage.query.Param;
import org.dbsyncer.storage.query.Query;
import org.dbsyncer.storage.util.ParamsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/10 23:22
 */
@Component("diskStorageServiceImpl")
@ConditionalOnProperty(value = "dbsyncer.storage.support.disk")
public class DiskStorageServiceImpl extends AbstractStorageService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, Shard> map = new ConcurrentHashMap();

    // 相对路径：./data/
    private static final String PATH = "data" + File.separator;

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
    public List<Map> select(String collectionId, Query query) throws IOException {
        Shard shard = map.get(collectionId);

        // 检查是否存在历史
        if (null == shard) {
            shard = cacheShardIfExist(collectionId);
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
                params.forEach(p -> builder.add(new TermQuery(new Term(p.getKey(), p.getValue())), BooleanClause.Occur.MUST));
                BooleanQuery q = builder.build();
                return shard.query(q, pageNum, pageSize, sort);
            }

            return shard.query(new MatchAllDocsQuery(), pageNum, pageSize, sort);
        }
        return Collections.emptyList();
    }

    @Override
    public void insert(String collectionId, Map params) throws IOException {
        createShardIfNotExist(collectionId);
        Document doc = ParamsUtil.convertParams2Doc(params);
        map.get(collectionId).insert(doc);
    }

    @Override
    public void update(String collectionId, Map params) throws IOException {
        createShardIfNotExist(collectionId);
        Document doc = ParamsUtil.convertParams2Doc(params);
        IndexableField field = doc.getField(ConfigConstant.CONFIG_MODEL_ID);
        map.get(collectionId).update(new Term(ConfigConstant.CONFIG_MODEL_ID, field.stringValue()), doc);
    }

    @Override
    public void delete(String collectionId, String id) throws IOException {
        createShardIfNotExist(collectionId);
        map.get(collectionId).delete(new Term(ConfigConstant.CONFIG_MODEL_ID, id));
    }

    @Override
    public void deleteAll(String collectionId) throws IOException {
        synchronized (this){
            Shard shard = map.get(collectionId);
            if (null != shard) {
                shard.deleteAll();
                map.remove(collectionId);
            }
        }
    }

    @Override
    public void insertLog(String collectionId, Map<String, Object> params) throws IOException {
        createShardIfNotExist(collectionId);
        Document doc = ParamsUtil.convertLog2Doc(params);
        map.get(collectionId).insert(doc);
    }

    @Override
    public void insertData(String collectionId, List<Map> list) throws IOException {
        createShardIfNotExist(collectionId);
        List<Document> docs = list.parallelStream().map(r -> ParamsUtil.convertData2Doc(r)).collect(Collectors.toList());
        map.get(collectionId).insertBatch(docs);
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
        if (null == map.get(collectionId)) {
            map.putIfAbsent(collectionId, new Shard(PATH + collectionId));
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

}