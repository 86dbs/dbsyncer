/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.config.PackageFormatConfig;
import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.PackageZipUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.TaskSplitUtil;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.OperationConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.enums.SortEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.BooleanFilter;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.filter.impl.StringFilter;
import org.dbsyncer.sdk.storage.SqlQuery;
import org.dbsyncer.sdk.storage.StorageService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * {@link TableGroupProfile} 实现（dbsyncer_table_group）。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public class TableGroupProfileImpl implements TableGroupProfile {

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private StorageService storageService;

    @Resource
    private MetaProfile metaProfile;

    @Override
    public String addTableGroup(TableGroup model) {
        String id = operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD));
        addTableGroupDetailMeta(id);
        return id;
    }

    @Override
    public void addTableGroupBatch(List<TableGroup> models) {
        if (CollectionUtils.isEmpty(models)) {
            return;
        }
        TaskSplitUtil.split(models, ConfigConstant.PAGE_SIZE, batch -> {
            List<String> ids = operationTemplate.executeBatch(batch, CommandEnum.OPR_ADD);
            List<Meta> metas = new ArrayList<>(ids.size());
            long now = System.currentTimeMillis();
            for (String id : ids) {
                if (StringUtil.isBlank(id)) {
                    continue;
                }
                Meta meta = new Meta();
                // id 由 OperationTemplate ADD 统一生成雪花；taskId 关联 table_group.id
                meta.setTaskId(id);
                meta.setIsTaskDetail(TaskLevelEnum.TASK_DETAIL.getCode());
                meta.setCreateTime(now);
                meta.setUpdateTime(now);
                metas.add(meta);
            }
            if (!CollectionUtils.isEmpty(metas)) {
                metaProfile.addMetaBatch(metas);
            }
        });
    }

    @Override
    public String editTableGroup(TableGroup model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_EDIT));
    }

    @Override
    public void removeTableGroup(String id) {
        removeTableGroupDetailMeta(id);
        storageService.remove(StorageEnum.TABLE_GROUP, id);
    }

    @Override
    public void removeTableGroupsByTaskId(String taskId) {
        if (StringUtil.isBlank(taskId)) {
            return;
        }
        List<String> groupIds = listTableGroupIds(taskId);
        //删除所有子任务
        metaProfile.deleteMetaByTableGroupIds(groupIds);
        Query deleteQuery = new Query();
        deleteQuery.setType(StorageEnum.TABLE_GROUP);
        deleteQuery.addFilter(ConfigConstant.TABLE_GROUP_TASK_ID, taskId);
        storageService.delete(deleteQuery);
    }

    @Override
    public TableGroup getTableGroup(String tableGroupId) {
        return operationTemplate.queryObject(TableGroup.class, tableGroupId);
    }

    @Override
    public Paging<TableGroup> queryTableGroup(String mappingId, String searchKey, int pageNum, int pageSize) {
        Paging<TableGroup> empty = new Paging<>(Math.max(pageNum, 1), normalizePageSize(pageSize));
        if (StringUtil.isBlank(mappingId)) {
            return empty;
        }
        int safePageNum = Math.max(pageNum, 1);
        int safePageSize = normalizePageSize(pageSize);
        Query query = new Query(safePageNum, safePageSize);
        query.setType(StorageEnum.TABLE_GROUP);
        query.addOrderBy(ConfigConstant.TABLE_GROUP_SORT_INDEX, SortEnum.DESC);
        if (StringUtil.isBlank(searchKey)) {
            query.addFilter(ConfigConstant.TABLE_GROUP_TASK_ID, mappingId);
        } else {
            BooleanFilter root = new BooleanFilter();
            BooleanFilter taskClause = new BooleanFilter();
            taskClause.add(new StringFilter(ConfigConstant.TABLE_GROUP_TASK_ID, FilterEnum.EQUAL, mappingId, false));
            BooleanFilter searchClause = new BooleanFilter();
            searchClause.add(new StringFilter(ConfigConstant.TABLE_GROUP_SOURCE_TABLE, FilterEnum.LIKE, searchKey, false));
            searchClause.or(new StringFilter(ConfigConstant.TABLE_GROUP_TARGET_TABLE, FilterEnum.LIKE, searchKey, false));
            root.add(taskClause, OperationEnum.AND);
            root.add(searchClause, OperationEnum.AND);
            query.setBooleanFilter(root);
        }
        Paging paging = storageService.query(query);
        Paging<TableGroup> result = new Paging<>(safePageNum, safePageSize);
        if (paging == null) {
            return result;
        }
        result.setTotal(paging.getTotal());
        if (CollectionUtils.isEmpty(paging.getData())) {
            return result;
        }
        List<TableGroup> groups = new ArrayList<>(paging.getData().size());
        for (Object item : paging.getData()) {
            TableGroup group = ConfigModelUtil.parseFromRow((Map) item, TableGroup.class);
            if (group != null) {
                groups.add(group);
            }
        }
        result.setData(groups);
        return result;
    }

    @Override
    public void forEachTableGroupPage(String mappingId, int pageSize, Consumer<List<TableGroup>> pageConsumer) {
        if (StringUtil.isBlank(mappingId) || pageConsumer == null) {
            return;
        }
        int safePageSize = normalizePageSize(pageSize);
        int pageNum = 1;
        while (true) {
            Paging<TableGroup> paging = queryTableGroup(mappingId, null, pageNum, safePageSize);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            List<TableGroup> page = new ArrayList<>(paging.getData());
            pageConsumer.accept(page);
            if (page.size() < safePageSize) {
                break;
            }
            pageNum++;
        }
    }

    @Override
    public List<TableGroup> listTableGroupsBySql(SqlQuery query) {
        if (query == null) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> rows = storageService.queryList(query);
        if (CollectionUtils.isEmpty(rows)) {
            return Collections.emptyList();
        }
        List<TableGroup> result = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            TableGroup group = ConfigModelUtil.parseFromRow(row, TableGroup.class);
            if (group != null) {
                result.add(group);
            }
        }
        return result;
    }

    @Override
    public int getTableGroupCount(String mappingId) {
        if (StringUtil.isBlank(mappingId)) {
            return 0;
        }
        Query condition = new Query();
        condition.addFilter(ConfigConstant.TABLE_GROUP_TASK_ID, mappingId);
        return operationTemplate.count(StorageEnum.TABLE_GROUP, condition);
    }

    private void addTableGroupDetailMeta(String tableGroupId) {
        if (StringUtil.isBlank(tableGroupId)) {
            return;
        }
        Meta existing = metaProfile.getMetaByTaskId(tableGroupId, TaskLevelEnum.TASK_DETAIL);
        if (existing != null) {
            return;
        }
        Meta meta = new Meta();
        meta.setTaskId(tableGroupId);
        meta.setIsTaskDetail(TaskLevelEnum.TASK_DETAIL.getCode());
        long now = System.currentTimeMillis();
        meta.setCreateTime(now);
        meta.setUpdateTime(now);
        metaProfile.addMeta(meta);
    }

    /**
     * 查询任务下全部 table_group.id。
     */
    @Override
    public List<String> listTableGroupIds(String taskId) {
        List<String> groupIds = new ArrayList<>();
        if (StringUtil.isBlank(taskId)) {
            return groupIds;
        }
        Query query = new Query();
        query.setType(StorageEnum.TABLE_GROUP);
        query.setPageSize(ConfigConstant.PAGE_SIZE);
        Set<String> selectFields = new HashSet<>();
        selectFields.add(ConfigConstant.CONFIG_MODEL_ID);
        query.setSelectFlied(selectFields);
        query.addFilter(ConfigConstant.TABLE_GROUP_TASK_ID, taskId);
        while (true) {
            Paging paging = storageService.query(query);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            for (Object item : paging.getData()) {
                Map<String, Object> row = (Map<String, Object>) item;
                groupIds.add(String.valueOf(row.get(ConfigConstant.CONFIG_MODEL_ID)));
            }
            query.setPageNum(query.getPageNum() + 1);
        }
        return groupIds;
    }

    @Override
    public List<TableGroup> listTableGroupAll() {
        return operationTemplate.queryList(StorageEnum.TABLE_GROUP, null, TableGroup.class);
    }

    @Override
    public void importTableGroupBatch(List<TableGroup> models) {
        if (CollectionUtils.isEmpty(models)) {
            return;
        }
        TaskSplitUtil.split(models, ConfigConstant.PAGE_SIZE, batch ->
                operationTemplate.executeBatch(batch, CommandEnum.OPR_ADD));
    }

    @Override
    public void importTableGroupNdjsonLines(List<String> ndjsonLines) {
        if (CollectionUtils.isEmpty(ndjsonLines)) {
            return;
        }
        List<TableGroup> buffer = new ArrayList<>(PackageFormatConfig.IMPORT_BATCH_SIZE);
        for (String line : ndjsonLines) {
            if (StringUtil.isBlank(line)) {
                continue;
            }
            TableGroup tg = JsonUtil.jsonToObj(line, TableGroup.class);
            if (tg == null) {
                continue;
            }
            buffer.add(tg);
            if (buffer.size() >= PackageFormatConfig.IMPORT_BATCH_SIZE) {
                importTableGroupBatch(new ArrayList<>(buffer));
                buffer.clear();
            }
        }
        if (!CollectionUtils.isEmpty(buffer)) {
            importTableGroupBatch(new ArrayList<>(buffer));
        }
    }

    @Override
    public void importFromZip(ZipFile zip) throws IOException {
        if (zip == null) {
            return;
        }
        List<String> buffer = new ArrayList<>(PackageFormatConfig.IMPORT_BATCH_SIZE);
        PackageZipUtil.forEachTableGroupNdjsonLine(zip, line -> {
            buffer.add(line);
            if (buffer.size() >= PackageFormatConfig.IMPORT_BATCH_SIZE) {
                importTableGroupNdjsonLines(new ArrayList<>(buffer));
                buffer.clear();
            }
        });
        if (!CollectionUtils.isEmpty(buffer)) {
            importTableGroupNdjsonLines(buffer);
        }
    }

    @Override
    public int writeTableGroupsToZip(ZipOutputStream zos) throws IOException {
        if (zos == null) {
            return 0;
        }
        String[] currentTaskId = {null};
        BufferedWriter[] writer = {null};
        int[] count = {0};
        try {
            forEachTableGroupSortedByTaskId(tg -> {
                try {
                    if (!StringUtil.equals(currentTaskId[0], tg.getTaskId())) {
                        flushWriter(writer[0]);
                        if (currentTaskId[0] != null) {
                            zos.closeEntry();
                        }
                        currentTaskId[0] = tg.getTaskId();
                        zos.putNextEntry(new ZipEntry(PackageFormatConfig.TABLE_GROUP_DIR + currentTaskId[0] + PackageFormatConfig.NDJSON_SUFFIX));
                        writer[0] = new BufferedWriter(new OutputStreamWriter(zos, StandardCharsets.UTF_8));
                    }
                    writer[0].write(JsonUtil.objToJson(tg));
                    writer[0].newLine();
                    count[0]++;
                } catch (IOException e) {
                    throw new ParserException("导出 table_group 失败: " + e.getMessage(), e);
                }
            });
        } finally {
            flushWriter(writer[0]);
            if (currentTaskId[0] != null) {
                zos.closeEntry();
            }
        }
        return count[0];
    }

    @Override
    public int countTableGroups() {
        return operationTemplate.count(StorageEnum.TABLE_GROUP, null);
    }

    @Override
    public void forEachTableGroupSortedByTaskId(Consumer<TableGroup> consumer) {
        if (consumer == null) {
            return;
        }
        Query query = new Query();
        query.setType(StorageEnum.TABLE_GROUP);
        query.setPageSize(ConfigConstant.PAGE_SIZE);
        query.addOrderBy(ConfigConstant.TABLE_GROUP_TASK_ID, SortEnum.ASC);
        while (true) {
            Paging paging = storageService.query(query);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            List<Map> data = (List<Map>) paging.getData();
            for (Map row : data) {
                TableGroup tg = ConfigModelUtil.parseFromRow(row, TableGroup.class);
                if (tg != null && StringUtil.isNotBlank(tg.getTaskId())) {
                    consumer.accept(tg);
                }
            }
            query.setPageNum(query.getPageNum() + 1);
        }
    }

    @Override
    public String getPreloadGroupKey(String taskId) {
        return ConfigConstant.TABLE_GROUP + "_" + taskId;
    }

    private void removeTableGroupDetailMeta(String tableGroupId) {
        if (StringUtil.isBlank(tableGroupId)) {
            return;
        }
        Meta byRef = metaProfile.getMetaByTaskId(tableGroupId, TaskLevelEnum.TASK_DETAIL);
        if (byRef != null && StringUtil.isNotBlank(byRef.getId())) {
            metaProfile.removeMeta(byRef.getId());
        }
    }

    private static int normalizePageSize(int pageSize) {
        return pageSize > 0 ? pageSize : ConfigConstant.PAGE_SIZE;
    }

    private static void flushWriter(BufferedWriter writer) throws IOException {
        if (writer != null) {
            writer.flush();
        }
    }

}
