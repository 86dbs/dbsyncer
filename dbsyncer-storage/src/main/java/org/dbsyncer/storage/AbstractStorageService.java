package org.dbsyncer.storage;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.query.BooleanFilter;
import org.dbsyncer.storage.query.Query;
import org.dbsyncer.storage.strategy.Strategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/16 1:28
 */
public abstract class AbstractStorageService implements StorageService, DisposableBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Map<String, Strategy> map;

    private final Lock lock = new ReentrantLock();

    protected abstract Paging select(String sharding, Query query);

    protected abstract void delete(String sharding, Query query);

    protected abstract void deleteAll(String sharding);

    protected abstract void batchInsert(StorageEnum type, String sharding, List<Map> list);

    protected abstract void batchUpdate(StorageEnum type, String sharding, List<Map> list);

    protected abstract void batchDelete(StorageEnum type, String sharding, List<String> ids);

    protected String getSharding(StorageEnum type, String collectionId) {
        Assert.notNull(type, "StorageEnum type can not be null.");
        Strategy strategy = map.get(type.getType().concat("Strategy"));
        Assert.notNull(strategy, "Strategy does not exist.");
        return strategy.createSharding(getSeparator(), collectionId);
    }

    protected String getSeparator() {
        return File.separator;
    }

    @Override
    public Paging query(Query query) {
        boolean locked = false;
        try {
            locked = lock.tryLock(3, TimeUnit.SECONDS);
            if (locked) {
                String sharding = getSharding(query.getType(), query.getMetaId());
                return select(sharding, query);
            }
        } catch (InterruptedException e) {
            logger.warn("tryLock error", e.getLocalizedMessage());
        } catch (IllegalArgumentException e) {
            logger.warn("查询数据异常，请重试");
        } finally {
            if (locked) {
                lock.unlock();
            }
        }
        return new Paging(query.getPageNum(), query.getPageSize());
    }

    @Override
    public void delete(Query query) {
        BooleanFilter q = query.getBooleanFilter();
        if (CollectionUtils.isEmpty(q.getClauses()) && CollectionUtils.isEmpty(q.getFilters())) {
            throw new StorageException("必须包含删除条件");
        }

        boolean locked = false;
        try {
            locked = lock.tryLock();
            if (locked) {
                String sharding = getSharding(query.getType(), query.getMetaId());
                delete(sharding, query);
            }
        } finally {
            if (locked) {
                lock.unlock();
            }
        }
    }

    @Override
    public void clear(StorageEnum type, String metaId) {
        try {
            lock.lock();
            String sharding = getSharding(type, metaId);
            deleteAll(sharding);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void add(StorageEnum type, Map params) {
        add(type, null, params);
    }

    @Override
    public void add(StorageEnum type, String metaId, Map params) {
        addBatch(type, metaId, newArrayList(params));
    }

    @Override
    public void addBatch(StorageEnum type, List<Map> list) {
        addBatch(type, null, list);
    }

    @Override
    public void addBatch(StorageEnum type, String metaId, List<Map> list) {
        if (!CollectionUtils.isEmpty(list)) {
            batchInsert(type, getSharding(type, metaId), list);
        }
    }

    @Override
    public void edit(StorageEnum type, Map params) {
        edit(type, null, params);
    }

    @Override
    public void edit(StorageEnum type, String metaId, Map params) {
        editBatch(type, metaId, newArrayList(params));
    }

    @Override
    public void editBatch(StorageEnum type, List<Map> list) {
        editBatch(type, null, list);
    }

    @Override
    public void editBatch(StorageEnum type, String metaId, List<Map> list) {
        if (!CollectionUtils.isEmpty(list)) {
            batchUpdate(type, getSharding(type, metaId), list);
        }
    }

    @Override
    public void remove(StorageEnum type, String id) {
        remove(type, null, id);
    }

    @Override
    public void remove(StorageEnum type, String metaId, String id) {
        removeBatch(type, metaId, newArrayList(id));
    }

    @Override
    public void removeBatch(StorageEnum type, List<String> ids) {
        removeBatch(type, null, ids);
    }

    @Override
    public void removeBatch(StorageEnum type, String metaId, List<String> ids) {
        if (!CollectionUtils.isEmpty(ids)) {
            batchDelete(type, getSharding(type, metaId), ids);
        }
    }

    private List<Map> newArrayList(Map params) {
        List<Map> list = new ArrayList<>();
        list.add(params);
        return list;
    }

    private List<String> newArrayList(String id) {
        List<String> list = new ArrayList<>();
        list.add(id);
        return list;
    }

}