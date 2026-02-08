/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.sdk.NullExecutorException;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.enums.StorageStrategyEnum;
import org.dbsyncer.sdk.filter.BooleanFilter;
import org.dbsyncer.sdk.filter.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2019-11-16 23:22
 */
public abstract class AbstractStorageService implements StorageService, DisposableBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Lock lock = new ReentrantLock();

    protected abstract Paging select(String sharding, Query query);

    protected abstract void delete(String sharding, Query query);

    protected abstract void deleteAll(String sharding);

    protected abstract void batchInsert(StorageEnum type, String sharding, List<Map> list);

    protected abstract void batchUpdate(StorageEnum type, String sharding, List<Map> list);

    protected abstract void batchDelete(StorageEnum type, String sharding, List<String> ids);

    protected String getSharding(StorageEnum type, String collectionId) {
        Assert.notNull(type, "StorageEnum type can not be null.");
        return StorageStrategyEnum.getStrategy(type).createSharding(getSeparator(), collectionId);
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
            logger.warn("tryLock error:{}", e.getLocalizedMessage());
        } catch (NullExecutorException e) {
            // 存储表不存在或已删除，请重试
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
            throw new SdkException("必须包含删除条件");
        }

        boolean locked = false;
        try {
            locked = lock.tryLock();
            if (locked) {
                String sharding = getSharding(query.getType(), query.getMetaId());
                delete(sharding, query);
            }
        } catch (NullExecutorException e) {
            // 存储表不存在或已删除，请重试
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
        } catch (NullExecutorException e) {
            // 存储表不存在或已删除，请重试
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
