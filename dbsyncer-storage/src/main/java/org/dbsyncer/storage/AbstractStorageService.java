package org.dbsyncer.storage;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.query.Query;
import org.dbsyncer.storage.strategy.Strategy;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/16 1:28
 */
public abstract class AbstractStorageService implements StorageService, DisposableBean {

    @Autowired
    private Map<String, Strategy> map;

    private final Lock lock = new ReentrantLock(true);

    private volatile boolean tryDeleteAll;

    protected abstract Paging select(String sharding, Query query);

    protected abstract void deleteAll(String sharding);

    protected abstract void batchInsert(StorageEnum type, String sharding, List<Map> list);

    protected abstract void batchUpdate(StorageEnum type, String sharding, List<Map> list);

    protected abstract void batchDelete(StorageEnum type, String sharding, List<String> list);

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
        if (tryDeleteAll) {
            return new Paging(query.getPageNum(), query.getPageSize());
        }

        boolean locked = false;
        try {
            locked = lock.tryLock();
            if (locked) {
                String sharding = getSharding(query.getType(), query.getMetaId());
                return select(sharding, query);
            }
        } finally {
            if (locked) {
                lock.unlock();
            }
        }
        return new Paging(query.getPageNum(), query.getPageSize());
    }

    @Override
    public void clear(StorageEnum type, String metaId) {
        boolean locked = false;
        try {
            locked = lock.tryLock();
            if (locked) {
                tryDeleteAll = true;
                String sharding = getSharding(type, metaId);
                deleteAll(sharding);
            }
        } finally {
            if (locked) {
                tryDeleteAll = false;
                lock.unlock();
            }
        }
    }

    @Override
    public void addConfig(Map params) {
        StorageEnum type = StorageEnum.CONFIG;
        batchInsert(type, getSharding(type, null), newArrayList(params));
    }

    @Override
    public void editConfig(Map params) {
        StorageEnum type = StorageEnum.CONFIG;
        batchUpdate(type, getSharding(type, null), newArrayList(params));
    }

    @Override
    public void removeConfig(String id) {
        StorageEnum type = StorageEnum.CONFIG;
        batchDelete(type, getSharding(type, null), newArrayList(id));
    }

    @Override
    public void addLog(Map<String, Object> params) {
        StorageEnum type = StorageEnum.LOG;
        batchInsert(type, getSharding(type, null), newArrayList(params));
    }

    @Override
    public void addData(String collectionId, List<Map> list) {
        StorageEnum type = StorageEnum.DATA;
        batchInsert(type, getSharding(type, collectionId), list);
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