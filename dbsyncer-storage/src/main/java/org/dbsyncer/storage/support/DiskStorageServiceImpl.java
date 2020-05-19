package org.dbsyncer.storage.support;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
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
    public List<Map> select(String collectionId, Query query) {
        Shard shard = map.get(collectionId);
        if(null != shard){
            try {
                List<Param> params = query.getParams();
                if (!CollectionUtils.isEmpty(params)) {
                    Param p = params.get(0);
                    Term term = new Term(p.getKey(), (String) p.getValue());
                    PrefixQuery q = new PrefixQuery(term);
                    return shard.prefixQuery(q);
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        return Collections.emptyList();
    }

    @Override
    public void insert(String collectionId, Map params) throws IOException {
        createShardIfNotExist(collectionId);
        Document doc = ParamsUtil.convertParamsToDocument(params);
        map.get(collectionId).insert(doc);
    }

    @Override
    public void update(String collectionId, Map params) throws IOException {
        createShardIfNotExist(collectionId);
        Document doc = ParamsUtil.convertParamsToDocument(params);
        IndexableField field = doc.getField(ConfigConstant.CONFIG_MODEL_ID);
        map.get(collectionId).update(new Term(ConfigConstant.CONFIG_MODEL_ID, field.stringValue()), doc);
    }

    @Override
    public void delete(String collectionId, String id) throws IOException {
        createShardIfNotExist(collectionId);
        map.get(collectionId).delete(new Term(ConfigConstant.CONFIG_MODEL_ID, id));
    }

    @Override
    public void insertLog(String collectionId, Map<String, Object> params) throws IOException {
        createShardIfNotExist(collectionId);
        // TODO 实现日志写入
    }

    @Override
    public void insertData(String collectionId, List<Map> list) throws IOException {
        createShardIfNotExist(collectionId);
        // TODO 实现数据写入

    }

    /**
     * 如果不存在分片则创建(线程安全)
     *<p>/data/config</p>
     *<p>/data/log</p>
     *<p>/data/data/123</p>
     *
     * @param collectionId
     * @throws IOException
     */
    private void createShardIfNotExist(String collectionId) throws IOException {
        if (null == map.get(collectionId)) {
            map.putIfAbsent(collectionId, new Shard(PATH + collectionId));
        }
    }

}