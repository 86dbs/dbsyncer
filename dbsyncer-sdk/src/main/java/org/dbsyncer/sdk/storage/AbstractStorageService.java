/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.NullExecutorException;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.NoticeFrequencyEnum;
import org.dbsyncer.sdk.enums.NoticeTypeEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.enums.StorageStrategyEnum;
import org.dbsyncer.sdk.filter.BooleanFilter;
import org.dbsyncer.sdk.filter.Query;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019-11-16 23:22
 */
public abstract class AbstractStorageService implements StorageService, DisposableBean {

    /**
     * 灌入/补齐默认告警策略（按通知类型 ID 幂等，已存在则跳过）。
     */
    protected void initNoticeStrategy() {
        long now = System.currentTimeMillis();
        String sharding = getSharding(StorageEnum.NOTICE_STRATEGY, null);
        List<Map> missing = new ArrayList<>(9);
        offerNoticeStrategyIfAbsent(missing, sharding, NoticeTypeEnum.TEST_MESSAGE, "测试消息",
                NoticeFrequencyEnum.IMMEDIATE, StringUtil.EMPTY, "立即通知", now);
        offerNoticeStrategyIfAbsent(missing, sharding, NoticeTypeEnum.CONNECTOR_OFFLINE, "连接离线",
                NoticeFrequencyEnum.IMMEDIATE, StringUtil.EMPTY, "5秒通知一次", now);
        offerNoticeStrategyIfAbsent(missing, sharding, NoticeTypeEnum.MAPPING_ERROR, "同步任务失败",
                NoticeFrequencyEnum.IMMEDIATE, StringUtil.EMPTY, "10分钟检测有失败数据,通知一次", now);
        offerNoticeStrategyIfAbsent(missing, sharding, NoticeTypeEnum.MAPPING_STOP, "手动停止同步任务",
                NoticeFrequencyEnum.IMMEDIATE, StringUtil.EMPTY, "立即通知", now);
        offerNoticeStrategyIfAbsent(missing, sharding, NoticeTypeEnum.VALIDATE_SYNC_FAIL, "订正校验失败",
                NoticeFrequencyEnum.IMMEDIATE, StringUtil.EMPTY, "任务执行完成,通知一次", now);
        offerNoticeStrategyIfAbsent(missing, sharding, NoticeTypeEnum.LICENSE_EXCEPTION, "授权异常",
                NoticeFrequencyEnum.INTERVAL, "0 */10 * * * ?", "十分钟通知一次", now);
        offerNoticeStrategyIfAbsent(missing, sharding, NoticeTypeEnum.LICENSE_EXPIRE_REMIND, "授权到期提醒",
                NoticeFrequencyEnum.INTERVAL, "0 0 9 * * ?", "每天通知一次", now);
        offerNoticeStrategyIfAbsent(missing, sharding, NoticeTypeEnum.LICENSE_EXPIRED, "授权已过期",
                NoticeFrequencyEnum.INTERVAL, "0 0 9 * * ?", "每天通知一次", now);
        offerNoticeStrategyIfAbsent(missing, sharding, NoticeTypeEnum.MAPPING_FULL_COMPLETE, "全量同步完成",
                NoticeFrequencyEnum.IMMEDIATE, StringUtil.EMPTY, "全量任务完成后通立即通知一次", now);
        if (!CollectionUtils.isEmpty(missing)) {
            batchInsert(StorageEnum.NOTICE_STRATEGY, sharding, missing);
        }
    }

    private void offerNoticeStrategyIfAbsent(List<Map> missing, String sharding, NoticeTypeEnum type,
                                             String name, NoticeFrequencyEnum frequency, String cron,
                                             String remark, long now) {
        Query query = new Query(1, 1);
        query.setType(StorageEnum.NOTICE_STRATEGY);
        query.addFilter(ConfigConstant.CONFIG_MODEL_ID, String.valueOf(type.getCode()));
        Map existing = null;
        try {
            existing = selectOne(sharding, query);
        } catch (NullExecutorException e) {
            // 表未就绪时跳过，下次启动再补
        }
        if (existing != null && !existing.isEmpty()) {
            return;
        }
        missing.add(noticeStrategyRow(type, name, frequency, cron, remark, now));
    }

    private static Map<String, Object> noticeStrategyRow(NoticeTypeEnum type, String name,
                                                         NoticeFrequencyEnum frequency, String cron,
                                                         String remark, long now) {
        Map<String, Object> row = new HashMap<>();
        row.put(ConfigConstant.CONFIG_MODEL_ID, String.valueOf(type.getCode()));
        row.put(ConfigConstant.CONFIG_MODEL_NAME, name);
        row.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, now);
        row.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, now);
        row.put(ConfigConstant.NOTICE_STRATEGY_ENABLED, 1);
        row.put(ConfigConstant.NOTICE_STRATEGY_FREQUENCY, frequency.getCode());
        row.put(ConfigConstant.NOTICE_STRATEGY_CRON, cron);
        row.put(ConfigConstant.NOTICE_STRATEGY_NOTICE_TYPES, type.getCode());
        row.put(ConfigConstant.NOTICE_STRATEGY_REMARK, StringUtil.getIfBlank(remark, StringUtil.EMPTY));
        return row;
    }

    protected abstract Paging select(String sharding, Query query);

    /**
     * 按条件查询单条
     */
    protected abstract Map selectOne(String sharding, Query query);

    protected abstract void delete(String sharding, Query query);

    protected abstract void deleteAll(String sharding);

    /**
     * 清空分片表数据并保留表结构（TRUNCATE）。
     */
    protected abstract void truncateAll(String sharding);

    protected abstract void batchInsert(StorageEnum type, String sharding, List<Map> list);

    protected abstract void batchUpdate(StorageEnum type, String sharding, List<Map> list);

    protected abstract void batchDelete(StorageEnum type, String sharding, List<String> ids);

    protected abstract void batchIncrement(StorageEnum type, String sharding, String id, Map<String, Long> deltas);

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
            String sharding = getSharding(query.getType(), query.getTaskId());
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
    public Map queryObject(Query query) {
        Assert.notNull(query, "Query can not be null.");
        Assert.notNull(query.getType(), "StorageEnum type can not be null.");
        BooleanFilter filter = query.getBooleanFilter();
        Assert.isTrue(filter != null
                        && (!CollectionUtils.isEmpty(filter.getFilters()) || !CollectionUtils.isEmpty(filter.getClauses())),
                "queryObject must contain filter conditions.");
        try {
            query.setPageNum(1);
            query.setPageSize(1);
            return selectOne(getSharding(query.getType(), query.getTaskId()), query);
        } catch (NullExecutorException e) {
            // 存储表不存在或已删除，请重试
        } catch (SdkException e) {
            if (isMissingStorageTable(e)) {
                return null;
            }
            throw e;
        }
        return null;
    }

    @Override
    public void delete(Query query) {
        BooleanFilter q = query.getBooleanFilter();
        if (CollectionUtils.isEmpty(q.getClauses()) && CollectionUtils.isEmpty(q.getFilters())) {
            throw new SdkException("必须包含删除条件");
        }

        try {
            String sharding = getSharding(query.getType(), query.getTaskId());
            delete(sharding, query);
        } catch (NullExecutorException e) {
            // 存储表不存在或已删除，请重试
        }
    }

    @Override
    public void clear(StorageEnum type, String taskId) {
        try {
            String sharding = getSharding(type, taskId);
            if (type == StorageEnum.TASK_DETAIL) {
                // 任务仍在：保证分表存在后只清数据，避免 DROP + 重建
                ensureShard(type, sharding);
                truncateAll(sharding);
            } else {
                deleteAll(sharding);
            }
        } catch (NullExecutorException e) {
            // 存储表不存在或已删除，请重试
        }
    }

    @Override
    public void drop(StorageEnum type, String taskId) {
        try {
            deleteAll(getSharding(type, taskId));
        } catch (NullExecutorException e) {
            // 存储表不存在或已删除，请重试
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
    public void add(StorageEnum type, String taskId, Map params) {
        addBatch(type, taskId, newArrayList(params));
    }

    @Override
    public void addBatch(StorageEnum type, String taskId, List<Map> list) {
        if (!CollectionUtils.isEmpty(list)) {
            batchInsert(type, getSharding(type, taskId), list);
        }
    }

    @Override
    public void edit(StorageEnum type, Map params) {
        edit(type, null, params);
    }

    @Override
    public void edit(StorageEnum type, String taskId, Map params) {
        editBatch(type, taskId, newArrayList(params));
    }

    @Override
    public void editBatch(StorageEnum type, String taskId, List<Map> list) {
        if (!CollectionUtils.isEmpty(list)) {
            batchUpdate(type, getSharding(type, taskId), list);
        }
    }

    @Override
    public void remove(StorageEnum type, String id) {
        remove(type, null, id);
    }

    @Override
    public void remove(StorageEnum type, String taskId, String id) {
        removeBatch(type, taskId, newArrayList(id));
    }

    @Override
    public void removeBatch(StorageEnum type, String taskId, List<String> ids) {
        if (!CollectionUtils.isEmpty(ids)) {
            batchDelete(type, getSharding(type, taskId), ids);
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
    public List<Map<String, Object>> queryList(ExecuteRequest query) {
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

    /**
     * 原生 SQL 查询。
     */
    protected abstract List<Map<String, Object>> selectList(String sql, Object[] args);

    /**
     * 原生 SQL 分页查询（由实现类按方言追加 LIMIT）。
     */
    protected abstract List<Map<String, Object>> selectList(String sql, int pageNum, int pageSize, Object[] args);

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
