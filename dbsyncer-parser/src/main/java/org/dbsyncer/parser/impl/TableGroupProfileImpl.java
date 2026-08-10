/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.TaskSplitUtil;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.OperationConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.parser.util.SqlResultRowUtil;
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
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * {@link TableGroupProfile} 实现（dbsyncer_table_group）。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public class TableGroupProfileImpl implements TableGroupProfile {

    private static final String RESULT_SELECT_COLUMNS =
            "tg.ID AS tableGroupId, "
                    + "tg.SOURCE_TABLE AS sourceTable, tg.TARGET_TABLE AS targetTable, "
                    + "COALESCE(dm.SUCCESS, 0) AS successTotal, COALESCE(dm.FAIL, 0) AS failTotal, "
                    + "COALESCE(dm.UPDATE_TIME, tg.UPDATE_TIME) AS updateTime";

    private static final String RESULT_FROM_JOIN =
            " FROM dbsyncer_table_group tg "
                    + "LEFT JOIN dbsyncer_meta dm ON dm.TASK_ID = tg.ID AND dm.IS_TASK_DETAIL = 1 ";

    private static final String RESULT_ORDER_SQL =
            " ORDER BY COALESCE(dm.UPDATE_TIME, tg.UPDATE_TIME) DESC,"
                    + " COALESCE(dm.FAIL, 0) DESC,"
                    + " COALESCE(dm.SUCCESS, 0) DESC";

    private static final String[] RESULT_SELECT_ALIASES = {
            ConfigConstant.DATA_TABLE_GROUP_ID,
            ConfigConstant.TABLE_GROUP_SOURCE_TABLE,
            ConfigConstant.TABLE_GROUP_TARGET_TABLE,
            ConfigConstant.DATABASE_SYNC_DETAIL_SUCCESS_TOTAL,
            ConfigConstant.DATABASE_SYNC_DETAIL_FAIL_TOTAL,
            ConfigConstant.CONFIG_MODEL_UPDATE_TIME
    };

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
        int safePageSize = pageSize > 0 ? pageSize : ConfigConstant.PAGE_SIZE;
        int safePageNum = Math.max(pageNum, 1);
        if (StringUtil.isBlank(mappingId)) {
            return new Paging<>(safePageNum, safePageSize);
        }
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
    public Paging queryTableGroupResults(String mappingId, String detailStatus, int pageNum, int pageSize) {
        Assert.hasText(mappingId, "驱动ID不能为空");
        int safePageNum = Math.max(pageNum, 1);
        int safePageSize = pageSize > 0 ? pageSize : ConfigConstant.PAGE_SIZE;

        List<Object> args = new ArrayList<>();
        String where = buildTableGroupResultWhere(mappingId, detailStatus, args);
        long total = queryTableGroupResultCount(where, args);
        Paging paging = new Paging(safePageNum, safePageSize);
        paging.setTotal(total);
        if (total <= 0) {
            paging.setData(Collections.emptyList());
            return paging;
        }
        String sql = "SELECT " + RESULT_SELECT_COLUMNS + RESULT_FROM_JOIN + where + RESULT_ORDER_SQL;
        List<Map<String, Object>> rows = storageService.queryList(
                SqlQuery.of(sql, args.toArray()).page(safePageNum, safePageSize));
        List<Map<String, Object>> data = new ArrayList<>(rows == null ? 0 : rows.size());
        if (!CollectionUtils.isEmpty(rows)) {
            for (Map<String, Object> row : rows) {
                data.add(SqlResultRowUtil.toAliasRow(row, RESULT_SELECT_ALIASES));
            }
        }
        paging.setData(data);
        return paging;
    }

    private String buildTableGroupResultWhere(String mappingId, String detailStatus, List<Object> args) {
        StringBuilder where = new StringBuilder("WHERE tg.TASK_ID = ? ");
        args.add(mappingId);
        if (StringUtil.equals("fail", detailStatus)) {
            where.append("AND COALESCE(dm.FAIL, 0) > 0 ");
        } else if (StringUtil.equals("success", detailStatus)) {
            where.append("AND COALESCE(dm.FAIL, 0) = 0 ");
        }
        return where.toString();
    }

    private long queryTableGroupResultCount(String where, List<Object> args) {
        String sql = "SELECT COUNT(1) AS cnt " + RESULT_FROM_JOIN + where;
        List<Map<String, Object>> rows = storageService.queryList(SqlQuery.of(sql, args.toArray()));
        if (CollectionUtils.isEmpty(rows)) {
            return 0L;
        }
        Object cnt = rows.get(0).get("cnt");
        if (cnt == null) {
            cnt = rows.get(0).values().iterator().next();
        }
        return NumberUtil.toLong(String.valueOf(cnt));
    }

    @Override
    public void pageScanTableGroups(String mappingId, int pageSize, Consumer<List<TableGroup>> pageConsumer) {
        if (StringUtil.isBlank(mappingId) || pageConsumer == null) {
            return;
        }
        int safePageSize = pageSize > 0 ? pageSize : ConfigConstant.PAGE_SIZE;
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

    @Override
    public boolean existsTableGroup(String taskId, String sourceTable, String targetTable) {
        if (StringUtil.isBlank(taskId) || StringUtil.isBlank(sourceTable) || StringUtil.isBlank(targetTable)) {
            return false;
        }
        Query condition = new Query();
        condition.addFilter(ConfigConstant.TABLE_GROUP_TASK_ID, taskId);
        condition.addFilter(ConfigConstant.TABLE_GROUP_SOURCE_TABLE, sourceTable);
        condition.addFilter(ConfigConstant.TABLE_GROUP_TARGET_TABLE, targetTable);
        return operationTemplate.count(StorageEnum.TABLE_GROUP, condition) > 0;
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
    public void addTableGroupBatchWithoutMeta(List<TableGroup> models) {
        if (CollectionUtils.isEmpty(models)) {
            return;
        }
        long now = System.currentTimeMillis();
        for (TableGroup model : models) {
            if (model == null) {
                continue;
            }
            // 配置包 NDJSON 可能缺 createTime/updateTime，库列 NOT NULL
            if (model.getCreateTime() == null) {
                model.setCreateTime(now);
            }
            if (model.getUpdateTime() == null) {
                model.setUpdateTime(now);
            }
        }
        TaskSplitUtil.split(models, ConfigConstant.PAGE_SIZE, batch ->
                operationTemplate.executeBatch(batch, CommandEnum.OPR_ADD));
    }

    @Override
    public int countTableGroups() {
        return operationTemplate.count(StorageEnum.TABLE_GROUP, null);
    }

    @Override
    public void pageScanTableGroupsByTaskId(Consumer<TableGroup> consumer) {
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

}
