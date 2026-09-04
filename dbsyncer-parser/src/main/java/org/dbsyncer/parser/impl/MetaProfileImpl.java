/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.config.PackageFormatConfig;
import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.TaskSplitUtil;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.OperationConfig;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.model.MetaIncrement;
import org.dbsyncer.sdk.storage.StorageService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * {@link MetaProfile} 实现（dbsyncer_meta）。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public class MetaProfileImpl implements MetaProfile {


    @Resource
    private StorageService storageService;

    @Resource
    private OperationTemplate operationTemplate;

    @Override
    public Meta getMeta(String metaId) {
        return operationTemplate.queryObject(Meta.class, metaId);
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
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
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
    public Meta getMetaByTaskId(String refId, TaskLevelEnum taskLevelEnum) {

        Query query = new Query(1, 1);
        query.setType(StorageEnum.META);
        query.addFilter(ConfigConstant.META_TASK_ID, refId);
        query.addFilter(ConfigConstant.META_IS_TASK_DETAIL, taskLevelEnum.getCode());
        Paging paging = storageService.query(query);
        if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
            return null;
        }
        Object row = paging.getData().iterator().next();
        return ConfigModelUtil.parseFromRow((Map) row, Meta.class);
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
        if (increment == null || StringUtil.isBlank(increment.getMetaId())) {
            return;
        }
        Map<String, Long> deltas = increment.toDeltaMap();
        if (deltas.isEmpty()) {
            return;
        }
        storageService.increment(StorageEnum.META, increment.getMetaId(), deltas);
    }

    @Override
    public void updateMetaProgress(String metaId, int state, Map<String, String> snapshot) {
        if (StringUtil.isBlank(metaId)) {
            return;
        }
        Meta meta = getMeta(metaId);
        if (meta == null) {
            return;
        }
        meta.setState(state);
        meta.setSnapshot(snapshot != null ? snapshot : new HashMap<>());
        meta.setUpdateTime(System.currentTimeMillis());
        updateMeta(meta);
    }

    @Override
    public void deleteMetaByTableGroupIds(List<String> tableGroupIds) {
        if (CollectionUtils.isEmpty(tableGroupIds)) {
            return;
        }
        TaskSplitUtil.split(tableGroupIds, ConfigConstant.PAGE_SIZE, (batch) -> {
            Query query = new Query();
            query.setType(StorageEnum.META);
            query.addFilter(ConfigConstant.META_IS_TASK_DETAIL, 1);
            query.addFilter(ConfigConstant.META_TASK_ID, FilterEnum.IN, StringUtil.join(batch, StringUtil.COMMA));
            storageService.delete(query);
        });

    }

    @Override
    public String resolveTaskDetailShardId(Meta meta) {
        if (meta == null) {
            return null;
        }
        if (meta.isTaskDetail()) {
            throw new ParserException("明细分表分片键须使用任务级 Meta，不能传入表级 Meta");
        }
        return StringUtil.isNotBlank(meta.getTaskId()) ? meta.getTaskId() : meta.getId();
    }

    @Override
    public String resolveTaskDetailShardId(String metaId) {
        if (StringUtil.isBlank(metaId)) {
            return metaId;
        }
        Meta meta = getMeta(metaId);
        if (meta != null) {
            return resolveTaskDetailShardId(meta);
        }
        return metaId;
    }

    @Override
    public String addMeta(Meta meta) {
        return operationTemplate.execute(new OperationConfig(meta, CommandEnum.OPR_ADD));
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
        return operationTemplate.execute(new OperationConfig(meta, CommandEnum.OPR_EDIT));
    }

    @Override
    public void updateMetaBatch(List<Meta> metas) {
        if (CollectionUtils.isEmpty(metas)) {
            return;
        }
        TaskSplitUtil.split(metas, ConfigConstant.PAGE_SIZE, batch -> {
            List<Map> paramsList = new ArrayList<>(batch.size());
            for (Meta meta : batch) {
                if (meta == null || StringUtil.isBlank(meta.getId())) {
                    continue;
                }
                paramsList.add(ConfigModelUtil.convertModelToMap(meta));
            }
            if (!CollectionUtils.isEmpty(paramsList)) {
                storageService.editBatch(StorageEnum.META, null, paramsList);
            }
        });
    }

    @Override
    public void removeMeta(String id) {
        operationTemplate.remove(new OperationConfig(id));
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
}
