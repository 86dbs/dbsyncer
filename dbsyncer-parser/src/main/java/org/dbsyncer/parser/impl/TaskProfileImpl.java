/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.config.PackageFormatConfig;
import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.enums.CommonTaskTypeEnum;
import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.PackageZipUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.TaskSplitUtil;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TaskImportResult;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.SortEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.model.DatabaseSyncTask;
import org.dbsyncer.sdk.model.MetaIncrement;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.zip.ZipFile;

/**
 * {@link TaskProfile} 实现。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public class TaskProfileImpl implements TaskProfile {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private TableGroupProfile tableGroupProfile;

    @Resource
    private StorageService storageService;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Override
    public <T extends ConfigModel> T getTask(String id, Class<T> clazz) {
        return operationTemplate.queryObject(clazz, id);
    }

    @Override
    public <T extends ConfigModel> Paging<T> queryTasks(Class<T> clazz, int pageNum, int pageSize, String searchKey) {
        Assert.notNull(clazz, "Task class can not be null.");
        int safePageNum = pageNum > 0 ? pageNum : 1;
        int safePageSize = pageSize > 0 ? pageSize : ConfigConstant.PAGE_SIZE;
        try {
            ConfigModel probe = (ConfigModel) clazz.newInstance();
            Query query = new Query(safePageNum, safePageSize);
            query.setType(StorageEnum.TASK);
            if (StringUtil.isNotBlank(probe.getType())) {
                query.addFilter(ConfigConstant.CONFIG_MODEL_TYPE, probe.getType());
            }
            if (StringUtil.isNotBlank(searchKey)) {
                query.addFilter(ConfigConstant.CONFIG_MODEL_NAME, searchKey, false);
            }
            query.addOrderBy(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, SortEnum.DESC);
            Paging paging = storageService.query(query);
            Paging<T> result = new Paging<>(safePageNum, safePageSize);
            if (paging == null) {
                return result;
            }
            result.setTotal(paging.getTotal());
            if (CollectionUtils.isEmpty(paging.getData())) {
                return result;
            }
            List<T> tasks = new ArrayList<>(paging.getData().size());
            for (Object item : paging.getData()) {
                T task = ConfigModelUtil.parseFromRow((Map) item, clazz);
                if (task != null) {
                    tasks.add(task);
                }
            }
            result.setData(tasks);
            return result;
        } catch (Exception e) {
            throw new ParserException(e);
        }
    }

    @Override
    public <T extends ConfigModel> void pageScanTasks(Class<T> clazz, int pageSize, Consumer<List<T>> pageConsumer) {
        if (clazz == null || pageConsumer == null) {
            return;
        }
        int safePageSize = pageSize > 0 ? pageSize : ConfigConstant.PAGE_SIZE;
        int pageNum = 1;
        while (true) {
            Paging<T> paging = queryTasks(clazz, pageNum, safePageSize,null);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            List<T> page = new ArrayList<>(paging.getData());
            pageConsumer.accept(page);
            if (page.size() < safePageSize) {
                break;
            }
            pageNum++;
        }
    }

    @Override
    public String addTask(ConfigModel task) {
        Assert.notNull(task, "Task can not be null.");
        if (StringUtil.isBlank(task.getId())) {
            task.setId(String.valueOf(snowflakeIdWorker.nextId()));
        }
        storageService.add(StorageEnum.TASK, ConfigModelUtil.convertModelToMap(task));
        return task.getId();
    }

    @Override
    public String updateTask(ConfigModel task) {
        Assert.notNull(task, "Task can not be null.");
        Assert.hasText(task.getId(), "Task id can not be empty.");
        storageService.edit(StorageEnum.TASK, ConfigModelUtil.convertModelToMap(task));
        return task.getId();
    }

    @Override
    public void addTaskBatch(List<? extends ConfigModel> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }
        List<Map> paramsList = new ArrayList<>(tasks.size());
        for (ConfigModel task : tasks) {
            Assert.notNull(task, "Task can not be null.");
            if (StringUtil.isBlank(task.getId())) {
                task.setId(String.valueOf(snowflakeIdWorker.nextId()));
            }
            paramsList.add(ConfigModelUtil.convertModelToMap(task));
        }
        storageService.addBatch(StorageEnum.TASK, null, paramsList);
        tasks.stream().map(ConfigModel::getId).collect(Collectors.toList());
    }

    @Override
    public void deleteTask(String id) {
        if (StringUtil.isBlank(id)) {
            return;
        }
        storageService.remove(StorageEnum.TASK, id);
    }

    @Override
    public int countTasks(String type) {
        Query condition = new Query();
        if (StringUtil.isNotBlank(type)) {
            condition.addFilter(ConfigConstant.CONFIG_MODEL_TYPE, type);
        }
        return operationTemplate.count(StorageEnum.TASK, condition);
    }

    @Override
    public boolean existsTask(String id) {
        if (StringUtil.isBlank(id)) {
            return false;
        }
        Query query = new Query(1, 1);
        query.setType(StorageEnum.TASK);
        query.addFilter(ConfigConstant.CONFIG_MODEL_ID, id);
        Paging paging = storageService.query(query);
        return paging != null && !CollectionUtils.isEmpty(paging.getData());
    }

    @Override
    public int countAllTasks() {
        return countTasks(null);
    }

    @Override
    public List<Map<String, Object>> listAllTaskJsonMaps() {
        List<Map<String, Object>> result = new ArrayList<>();
        Query query = new Query();
        query.setType(StorageEnum.TASK);
        query.setPageSize(ConfigConstant.PAGE_SIZE);
        while (true) {
            Paging paging = storageService.query(query);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            List<Map> data = (List<Map>) paging.getData();
            for (Map row : data) {
                Object json = row.get(ConfigConstant.CONFIG_MODEL_JSON);
                if (json == null) {
                    continue;
                }
                Map<String, Object> task = JsonUtil.parseMap(String.valueOf(json));
                if (task != null) {
                    result.add(task);
                }
            }
            query.setPageNum(query.getPageNum() + 1);
        }
        return result;
    }

    @Override
    public String importTask(ConfigModel task) {
        Assert.notNull(task, "Task can not be null.");
        Assert.hasText(task.getId(), "Import task id can not be empty.");
        storageService.add(StorageEnum.TASK, ConfigModelUtil.convertModelToMap(task));
        return task.getId();
    }

    @Override
    public TaskImportResult importTasksFromJson(String json) {
        if (StringUtil.isBlank(json)) {
            return new TaskImportResult(0, Collections.emptyList());
        }
        List list = JsonUtil.parseList(json);
        if (CollectionUtils.isEmpty(list)) {
            return new TaskImportResult(0, Collections.emptyList());
        }
        List<Mapping> mappings = new ArrayList<>();
        List<ConfigModel> enterpriseTasks = new ArrayList<>();
        for (Object item : list) {
            Map map = item instanceof Map ? (Map) item : JsonUtil.parseMap(JsonUtil.objToJson(item));
            if (map == null) {
                continue;
            }
            String type = map.get(ConfigConstant.CONFIG_MODEL_TYPE) == null
                    ? null : String.valueOf(map.get(ConfigConstant.CONFIG_MODEL_TYPE));
            String itemJson = JsonUtil.objToJson(map);
            if (StringUtil.equals(ConfigConstant.MAPPING, type) || StringUtil.isBlank(type)) {
                Mapping mapping = JsonUtil.jsonToObj(itemJson, Mapping.class);
                if (mapping != null) {
                    if (StringUtil.isBlank(mapping.getType())) {
                        mapping.setType(ConfigConstant.MAPPING);
                    }
                    mappings.add(mapping);
                }
                continue;
            }
            CommonTaskTypeEnum taskType = CommonTaskTypeEnum.parse(type);
            if (taskType == CommonTaskTypeEnum.VALIDATE_SYNC) {
                ValidateSyncTask task = JsonUtil.jsonToObj(itemJson, ValidateSyncTask.class);
                if (task != null) {
                    importTask(task);
                    enterpriseTasks.add(task);
                }
            } else if (taskType == CommonTaskTypeEnum.DATABASE_SYNC) {
                DatabaseSyncTask task = JsonUtil.jsonToObj(itemJson, DatabaseSyncTask.class);
                if (task != null) {
                    importTask(task);
                    enterpriseTasks.add(task);
                }
            } else {
                logger.warn("跳过未知任务类型: type={}, id={}", type, map.get(ConfigConstant.CONFIG_MODEL_ID));
            }
        }
        if (!CollectionUtils.isEmpty(mappings)) {
            TaskSplitUtil.split(mappings, PackageFormatConfig.IMPORT_BATCH_SIZE, this::addTaskBatch);
        }
        return new TaskImportResult(mappings.size(), enterpriseTasks);
    }

    @Override
    public void importTaskDetailSchemasFromJson(String json) {
        if (StringUtil.isBlank(json)) {
            return;
        }
        Map map = JsonUtil.parseMap(json);
        if (map == null) {
            return;
        }
        Object taskIdsObj = map.get("taskIds");
        if (!(taskIdsObj instanceof List)) {
            return;
        }
        List<String> taskIds = new ArrayList<>();
        for (Object item : (List) taskIdsObj) {
            if (item == null) {
                continue;
            }
            String taskId = String.valueOf(item);
            if (StringUtil.isNotBlank(taskId)) {
                taskIds.add(taskId);
            }
        }
        createRunDetailTables(taskIds);
    }

    @Override
    public TaskImportResult importTasksFromZip(ZipFile zip) throws IOException {
        if (zip == null) {
            return new TaskImportResult(0, Collections.emptyList());
        }
        String json = PackageZipUtil.readOptionalEntry(zip, PackageFormatConfig.TASK);
        if (StringUtil.isBlank(json)) {
            json = PackageZipUtil.readOptionalEntry(zip, PackageFormatConfig.MAPPING);
        }
        return importTasksFromJson(json);
    }

    @Override
    public String exportTaskDetailSchemasJson(List<String> taskIds) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("taskIds", taskIds == null ? Collections.emptyList() : taskIds);
        return JsonUtil.objToJson(payload);
    }

    @Override
    public void deleteTableRunMeta(String taskId) {
        metaProfile.deleteMetaByTableGroupIds(tableGroupProfile.listTableGroupIds(taskId));
    }

    @Override
    public void clearRunData(String taskId) {
        if (StringUtil.isBlank(taskId)) {
            return;
        }
        List<String> groupIds = tableGroupProfile.listTableGroupIds(taskId);
        storageService.clear(StorageEnum.TASK_DETAIL, taskId);
        if (CollectionUtils.isEmpty(groupIds)) {
            return;
        }
        // 就地重置已有明细 Meta，避免 delete + insert 写放大；缺失的再补插
        Map<String, Meta> existing = metaProfile.getDetailMetaMap(groupIds);
        long now = System.currentTimeMillis();
        List<Meta> toUpdate = new ArrayList<>();
        List<Meta> toAdd = new ArrayList<>();
        for (String groupId : groupIds) {
            Meta meta = existing.get(groupId);
            if (meta != null) {
                if (isDetailMetaClean(meta)) {
                    continue;
                }
                resetDetailMeta(meta, groupId, now);
                toUpdate.add(meta);
            } else {
                Meta created = new Meta();
                resetDetailMeta(created, groupId, now);
                created.setCreateTime(now);
                toAdd.add(created);
            }
        }
        if (!CollectionUtils.isEmpty(toUpdate)) {
            metaProfile.updateMetaBatch(toUpdate);
        }
        if (!CollectionUtils.isEmpty(toAdd)) {
            TaskSplitUtil.split(toAdd, ConfigConstant.PAGE_SIZE, metaProfile::addMetaBatch);
        }
    }

    /**
     * 明细 Meta 归零：状态 READY、计数清零、快照清空。{@link Meta#clear()} 会把 isTaskDetail 置 0，须再写回明细层级。
     */
    private void resetDetailMeta(Meta meta, String groupId, long now) {
        meta.clear();
        meta.setTaskId(groupId);
        meta.setIsTaskDetail(TaskLevelEnum.TASK_DETAIL.getCode());
        meta.setUpdateTime(now);
    }

    private boolean isDetailMetaClean(Meta meta) {
        if (meta == null) {
            return false;
        }
        if (meta.getState() != CommonTaskStatusEnum.READY.getCode()) {
            return false;
        }
        if (counterValue(meta.getTotal()) != 0L || counterValue(meta.getSuccess()) != 0L
                || counterValue(meta.getFail()) != 0L || counterValue(meta.getDiff()) != 0L
                || counterValue(meta.getFixed()) != 0L) {
            return false;
        }
        return meta.getSnapshot() == null || meta.getSnapshot().isEmpty();
    }

    @Override
    public void createRunDetailTable(String taskId) {
        if (StringUtil.isBlank(taskId)) {
            return;
        }
        storageService.ensure(StorageEnum.TASK_DETAIL, taskId);
    }

    @Override
    public void createRunDetailTables(List<String> taskIds) {
        if (CollectionUtils.isEmpty(taskIds)) {
            return;
        }
        for (String taskId : taskIds) {
            createRunDetailTable(taskId);
        }
    }

    @Override
    public void resetRunProgress(String taskId) {
        if (StringUtil.isBlank(taskId)) {
            return;
        }
        Meta meta = metaProfile.getMetaByTaskId(taskId, TaskLevelEnum.TASK);
        if (meta == null) {
            return;
        }
        zeroTaskMetaCounters(meta);
        meta.clear();
        meta.setTaskId(taskId);
        meta.setIsTaskDetail(TaskLevelEnum.TASK.getCode());
        meta.setUpdateTime(System.currentTimeMillis());
        metaProfile.updateMeta(meta);
    }

    private void zeroTaskMetaCounters(Meta meta) {
        long total = counterValue(meta.getTotal());
        long success = counterValue(meta.getSuccess());
        long fail = counterValue(meta.getFail());
        long diff = counterValue(meta.getDiff());
        long fixed = counterValue(meta.getFixed());
        if (total == 0L && success == 0L && fail == 0L && diff == 0L && fixed == 0L) {
            return;
        }
        metaProfile.incrementMeta(MetaIncrement.of(meta.getId())
                .total(-total)
                .success(-success)
                .fail(-fail)
                .diff(-diff)
                .fixed(-fixed));
    }

    private static long counterValue(AtomicLong value) {
        return value == null ? 0L : value.get();
    }
}
