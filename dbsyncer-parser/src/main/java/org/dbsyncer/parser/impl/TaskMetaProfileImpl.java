/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.config.PackageFormatConfig;
import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.event.RemoveMetaCacheEvent;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.TaskSplitUtil;
import org.dbsyncer.parser.AbstractConfigModelProfile;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.TaskMetaProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.model.MetaIncrement;
import org.dbsyncer.sdk.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * {@link TaskMetaProfile} 实现（dbsyncer_meta）。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public final class TaskMetaProfileImpl extends AbstractConfigModelProfile<Meta> implements TaskMetaProfile, ApplicationListener<RemoveMetaCacheEvent> {

    private static final Logger log = LoggerFactory.getLogger(TaskMetaProfileImpl.class);
    @Resource
    private StorageService storageService;

    @Resource
    private OperationTemplate operationTemplate;

    @Override
    public Meta getMeta(String taskId) {
        return getCache(taskId);
    }

    @Override
    public Meta getCache(String taskId) {
        String cacheKey = buildCacheKey(taskId);
        Meta cached = cacheService.get(cacheKey, Meta.class);
        if (cached != null) {
            return cached;
        }

        return cacheService.executeWithLock(buildLockKey(taskId), () -> {
            Meta again = cacheService.get(cacheKey, Meta.class);
            if (again != null) {
                return again;
            }

            Query query = new Query();
            query.setType(StorageEnum.META);
            query.addFilter(ConfigConstant.TABLE_GROUP_TASK_ID, taskId);
            Map row = storageService.queryObject(query);
            if (CollectionUtils.isEmpty(row)) {
                return null;
            }
            Meta newMeta = ConfigModelUtil.parseFromRow(row, Meta.class);
            if (newMeta != null) {
                cacheService.put(cacheKey, newMeta, EXPIRED_1_HOURS);
            }
            return newMeta;
        });
    }

    @Override
    public Meta getMetaDetail(String taskId) {
        Query query = new Query();
        query.setType(StorageEnum.META);
        query.addFilter(ConfigConstant.META_TASK_ID, taskId);
        query.addFilter(ConfigConstant.META_IS_TASK_DETAIL, TaskLevelEnum.TASK_DETAIL.getCode());
        Map row = storageService.queryObject(query);
        if (row == null) {
            return null;
        }
        return ConfigModelUtil.parseFromRow(row, Meta.class);
    }

    @Override
    public Paging<Meta> queryMeta(Integer isTaskDetail, int pageNum, int pageSize) {
        int safePageNum = pageNum > 0 ? pageNum : 1;
        int safePageSize = pageSize > 0 ? pageSize : ConfigConstant.PAGE_SIZE;
        Query query = new Query(safePageNum, safePageSize);
        query.setType(StorageEnum.META);
        if (isTaskDetail != null) {
            query.addFilter(ConfigConstant.META_IS_TASK_DETAIL, isTaskDetail);
        }
        Paging paging = storageService.query(query);
        Paging<Meta> result = new Paging<>(safePageNum, safePageSize);
        if (paging == null) {
            return result;
        }
        result.setTotal(paging.getTotal());
        if (CollectionUtils.isEmpty(paging.getData())) {
            return result;
        }
        List<Meta> metas = new ArrayList<>(paging.getData().size());
        for (Object item : paging.getData()) {
            if (!(item instanceof Map)) {
                continue;
            }
            Meta meta = ConfigModelUtil.parseFromRow((Map) item, Meta.class);
            if (meta != null) {
                metas.add(meta);
            }
        }
        result.setData(metas);
        return result;
    }

    @Override
    public void pageScanMetas(Integer isTaskDetail, int pageSize, Consumer<List<Meta>> pageConsumer) {
        if (pageConsumer == null) {
            return;
        }
        int safePageSize = pageSize > 0 ? pageSize : ConfigConstant.PAGE_SIZE;
        int pageNum = 1;
        while (true) {
            Paging<Meta> paging = queryMeta(isTaskDetail, pageNum, safePageSize);
            if (CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            List<Meta> page = new ArrayList<>(paging.getData());
            pageConsumer.accept(page);
            if (page.size() < safePageSize) {
                break;
            }
            pageNum++;
        }
    }

    @Override
    public Map<String, Meta> getTaskMetaMap(List<String> taskIds) {
        return queryMetaMapByTaskIds(taskIds, TaskLevelEnum.TASK);
    }

    @Override
    public Map<String, Meta> getDetailMetaMap(List<String> refIds) {
        return queryMetaMapByTaskIds(refIds, TaskLevelEnum.TASK_DETAIL);
    }

    private Map<String, Meta> queryMetaMapByTaskIds(List<String> refIds, TaskLevelEnum taskLevelEnum) {
        Map<String, Meta> result = new java.util.HashMap<>();
        if (CollectionUtils.isEmpty(refIds) || taskLevelEnum == null) {
            return result;
        }
        List<String> ids = refIds.stream().filter(StringUtil::isNotBlank).distinct().collect(Collectors.toList());
        if (ids.isEmpty()) {
            return result;
        }
        TaskSplitUtil.split(ids, ConfigConstant.PAGE_SIZE, (batch) -> {
            Query query = new Query(1, batch.size());
            query.setType(StorageEnum.META);
            query.addFilter(ConfigConstant.META_IS_TASK_DETAIL, taskLevelEnum.getCode());
            query.addFilter(ConfigConstant.META_TASK_ID, FilterEnum.IN, String.join(StringUtil.COMMA, batch));
            Paging paging = storageService.query(query);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                return;
            }
            for (Object item : paging.getData()) {
                if (!(item instanceof Map)) {
                    continue;
                }
                Meta meta = ConfigModelUtil.parseFromRow((Map) item, Meta.class);
                if (meta != null && StringUtil.isNotBlank(meta.getTaskId())) {
                    result.put(meta.getTaskId(), meta);
                }
            }
        });
        return result;
    }

    @Override
    public void incrementMeta(MetaIncrement increment) {
        if (increment == null || StringUtil.isBlank(increment.getTaskId())) {
            return;
        }
        Map<String, Long> deltas = increment.toDeltaMap();
        if (deltas.isEmpty()) {
            return;
        }
        storageService.increment(StorageEnum.META, increment.getTaskId(), deltas);
        // 库内 UPDATE_TIME/计数已更新；同步内存缓存，供 flushEvent 20s 门控与页面进度读取
        syncCachedMetaAfterIncrement(increment);
    }

    /**
     * 将原子增量同步到已缓存的 Meta，避免缓存仍停留在启动时的 updateTime。
     */
    private void syncCachedMetaAfterIncrement(MetaIncrement increment) {
        Meta cached = cacheService.get(buildCacheKey(increment.getTaskId()), Meta.class);
        if (cached == null) {
            return;
        }
        cached.setUpdateTime(System.currentTimeMillis());
        addDelta(cached.getTotal(), increment.getTotalDelta());
        addDelta(cached.getSuccess(), increment.getSuccessDelta());
        addDelta(cached.getFail(), increment.getFailDelta());
        addDelta(cached.getDiff(), increment.getDiffDelta());
        addDelta(cached.getFixed(), increment.getFixedDelta());
    }

    private void addDelta(AtomicLong counter, long delta) {
        if (counter == null || delta == 0L) {
            return;
        }
        long next = counter.addAndGet(delta);
        if (next < 0L) {
            counter.set(0L);
        }
    }

    @Override
    public void updateMetaProgress(String taskId, int state, Map<String, String> snapshot) {
        if (StringUtil.isBlank(taskId)) {
            return;
        }
        Meta meta = getMeta(taskId);
        if (meta == null) {
            return;
        }
        meta.setState(state);
        meta.setSnapshot(snapshot != null ? snapshot : new HashMap<>());
        meta.setUpdateTime(System.currentTimeMillis());
        updateMeta(meta);
    }

    @Override
    public String addMeta(Meta meta) {
        return operationTemplate.execute(meta, CommandEnum.OPR_ADD);
    }

    @Override
    public void addMetaBatch(List<Meta> metas) {
        if (CollectionUtils.isEmpty(metas)) {
            return;
        }
        TaskSplitUtil.split(metas, ConfigConstant.PAGE_SIZE, batch ->
                operationTemplate.executeBatch(batch, CommandEnum.OPR_ADD));
    }

    @Override
    public String updateMeta(Meta meta) {
        String execute = operationTemplate.execute(meta, CommandEnum.OPR_EDIT);
        removeCacheAndNotice(meta.getTaskId());
        return execute;
    }

    @Override
    public void updateMetaWithoutNotice(Meta meta) {
        operationTemplate.execute(meta, CommandEnum.OPR_EDIT);
    }

    @Override
    public void removeMeta(String taskId) {
        Meta meta = getMeta(taskId);
        if (meta != null) {
            storageService.remove(StorageEnum.META, meta.getId());
            removeCacheAndNotice(taskId);
        }
    }

    @Override
    public int countMeta() {
        return operationTemplate.count(StorageEnum.META, null);
    }

    @Override
    public int writeMetasToZip(ZipOutputStream zos) throws IOException {
        if (zos == null) {
            return 0;
        }
        zos.putNextEntry(new ZipEntry(PackageFormatConfig.META));
        int[] count = {0};
        boolean[] first = {true};
        try {
            OutputStreamWriter writer = new OutputStreamWriter(zos, StandardCharsets.UTF_8);
            writer.write('[');
            pageScanMetas(null, ConfigConstant.PAGE_SIZE, page -> {
                try {
                    for (Meta meta : page) {
                        if (meta == null) {
                            continue;
                        }
                        if (!first[0]) {
                            writer.write(',');
                        }
                        first[0] = false;
                        writer.write(JsonUtil.objToJson(meta));
                        count[0]++;
                    }
                    writer.flush();
                } catch (IOException e) {
                    throw new ParserException("导出 meta 失败: " + e.getMessage(), e);
                }
            });
            writer.write(']');
            writer.flush();
        } catch (ParserException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        } finally {
            zos.closeEntry();
        }
        return count[0];
    }

    @Override
    public void importMetaFromJson(String json) {
        if (StringUtil.isBlank(json)) {
            return;
        }
        List<Meta> metas = JsonUtil.jsonToArray(json, Meta.class);
        if (CollectionUtils.isEmpty(metas)) {
            return;
        }
        if (metas.size() == 1) {
            addMeta(metas.get(0));
            return;
        }
        TaskSplitUtil.split(metas, PackageFormatConfig.IMPORT_BATCH_SIZE, this::addMetaBatch);
    }

    @Override
    public void removeMetaCache(String taskId) {
        removeCache(taskId);
    }

    @Override
    public void clearMeta(String taskId) {
        Meta meta = getMeta(taskId);
        if (meta != null) {
            meta.clear();
            meta.setUpdateTime(System.currentTimeMillis());
            updateMeta(meta);
        }
    }

    @Override
    public void resetMeta(String taskId) {
        Meta meta = getMeta(taskId);
        if (meta != null) {
            meta.reset();
            meta.setUpdateTime(System.currentTimeMillis());
            updateMeta(meta);
        }
    }

    @Override
    public void onApplicationEvent(RemoveMetaCacheEvent event) {
        removeMetaCache(event.getCommonMessage().getId());
    }
}
