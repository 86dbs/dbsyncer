/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.NullExecutorException;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.enums.StorageStrategyEnum;
import org.dbsyncer.sdk.filter.BooleanFilter;
import org.dbsyncer.sdk.filter.Query;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019-11-16 23:22
 */
public abstract class AbstractStorageService implements StorageService, DisposableBean {

    protected abstract Paging select(String sharding, Query query);

    protected abstract void delete(String sharding, Query query);

    protected abstract void deleteAll(String sharding);

    protected abstract void batchInsert(StorageEnum type, String sharding, List<Map> list);

    protected abstract void batchUpdate(StorageEnum type, String sharding, List<Map> list);

    protected abstract void batchDelete(StorageEnum type, String sharding, List<String> ids);

    protected abstract void batchIncrement(StorageEnum type, String sharding, String id, Map<String, Long> deltas);

    /**
     * 条件更新实现。
     *
     * @param increments 原子累加列，可为 null
     * @return 影响行数
     */
    protected abstract int compareAndUpdate(StorageEnum type, String sharding, String id, Map params,
                                           Map<String, Long> increments, String casField, Object casValue);

    protected String getSharding(StorageEnum type, String collectionId) {
        Assert.notNull(type, "StorageEnum type can not be null.");
        return StorageStrategyEnum.getStrategy(type).createSharding(getSeparator(), collectionId);
    }

    protected String getSeparator() {
        return File.separator;
    }

    @Override
    public Paging query(Query query) {
        try {
            String sharding = getSharding(query.getType(), query.getMetaId());
            return select(sharding, query);
        } catch (NullExecutorException e) {
            // 存储表不存在或已删除，请重试
        } catch (SdkException e) {
            // 动态分表尚未创建（任务未启动/无明细）时按空结果返回
            if (isMissingStorageTable(e)) {
                return new Paging(query.getPageNum(), query.getPageSize());
            }
            throw e;
        }
        return new Paging(query.getPageNum(), query.getPageSize());
    }

    @Override
    public void delete(Query query) {
        BooleanFilter q = query.getBooleanFilter();
        if (CollectionUtils.isEmpty(q.getClauses()) && CollectionUtils.isEmpty(q.getFilters())) {
            throw new SdkException("必须包含删除条件");
        }

        try {
            String sharding = getSharding(query.getType(), query.getMetaId());
            delete(sharding, query);
        } catch (NullExecutorException e) {
            // 存储表不存在或已删除，请重试
        }
    }

    @Override
    public void clear(StorageEnum type, String metaId) {
        try {
            String sharding = getSharding(type, metaId);
            deleteAll(sharding);
            // 动态明细分表：清空后预建空表，避免详情 JOIN 查询报 Table not found
            if (type == StorageEnum.TASK_DETAIL) {
                ensureShard(type, sharding);
            }
        } catch (NullExecutorException e) {
            // 存储表不存在或已删除，请重试
        }
    }

    @Override
    public void ensure(StorageEnum type, String metaId) {
        try {
            ensureShard(type, getSharding(type, metaId));
        } catch (NullExecutorException e) {
            // 未知存储类型等，忽略
        }
    }

    /**
     * 确保分片物理表存在（仅 DDL，不写数据）。
     *
     * @param type     存储类型
     * @param sharding 分片名（如 task_detail_{taskId}）
     */
    protected abstract void ensureShard(StorageEnum type, String sharding);

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

    @Override
    public void increment(StorageEnum type, String id, Map<String, Long> deltas) {
        try {
            batchIncrement(type, getSharding(type, null), id, deltas);
        } catch (NullExecutorException e) {
            // 存储表不存在或已删除，请重试
        }
    }

    @Override
    public int compareAndEdit(StorageEnum type, String id, Map params, String casField, Object casValue) {
        return compareAndEdit(type, id, params, null, casField, casValue);
    }

    @Override
    public int compareAndEdit(StorageEnum type, String id, Map params, Map<String, Long> increments,
                              String casField, Object casValue) {
        if (type == null || StringUtil.isBlank(id) || StringUtil.isBlank(casField)) {
            return 0;
        }
        if (CollectionUtils.isEmpty(params) && CollectionUtils.isEmpty(increments)) {
            return 0;
        }
        try {
            return compareAndUpdate(type, getSharding(type, null), id, params, increments, casField, casValue);
        } catch (NullExecutorException e) {
            return 0;
        }
    }

    @Override
    public List<Map<String, Object>> queryList(SqlQuery query) {
        Assert.notNull(query, "SqlQuery can not be null.");
        Assert.hasText(query.getSql(), "sql can not be empty.");
        try {
            if (query.isPaged()) {
                return selectList(query.getSql(), query.getPageNum(), query.getPageSize(), query.getArgs());
            }
            return selectList(query.getSql(), query.getArgs());
        } catch (NullExecutorException e) {
            return new ArrayList<>();
        }
    }

    @Override
    public int executeUpdate(SqlQuery query) {
        Assert.notNull(query, "SqlQuery can not be null.");
        Assert.hasText(query.getSql(), "sql can not be empty.");
        try {
            return update(query.getSql(), query.getArgs());
        } catch (NullExecutorException e) {
            return 0;
        }
    }

    /**
     * 原生 SQL 查询。
     */
    protected abstract List<Map<String, Object>> selectList(String sql, Object[] args);

    /**
     * 原生 SQL 分页查询（由实现类按方言追加 LIMIT）。
     */
    protected abstract List<Map<String, Object>> selectList(String sql, int pageNum, int pageSize, Object[] args);

    /**
     * 原生 SQL 更新。
     *
     * @param sql  SQL
     * @param args 绑定参数
     * @return 影响行数
     */
    protected abstract int update(String sql, Object[] args);

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

    private boolean isMissingStorageTable(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("doesn't exist") || msg.contains("Unknown table") || msg.contains("not found"))) {
                return true;
            }
        }
        return false;
    }
}
