/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.TaskSplitUtil;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.enums.GroupStrategyEnum;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.OperationConfig;
import org.dbsyncer.parser.model.QueryConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.storage.SqlQuery;
import org.dbsyncer.sdk.storage.StorageService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
        String id = operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD, GroupStrategyEnum.TABLE));
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
                operationTemplate.executeBatch(metas, CommandEnum.OPR_ADD);
            }
        });
    }

    @Override
    public String editTableGroup(TableGroup model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_EDIT, GroupStrategyEnum.TABLE));
    }

    @Override
    public void removeTableGroup(String id) {
        removeTableGroupDetailMeta(id);
        operationTemplate.remove(new OperationConfig(id, GroupStrategyEnum.TABLE));
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
    public List<TableGroup> getTableGroupAll(String mappingId) {
        TableGroup tableGroup = new TableGroup().setTaskId(mappingId);
        return operationTemplate.queryAll(new QueryConfig<>(tableGroup, GroupStrategyEnum.TABLE));
    }

    @Override
    public List<TableGroup> getSortedTableGroupAll(String mappingId) {
        return getTableGroupAll(mappingId).stream()
                .sorted(Comparator.comparing(TableGroup::getIndex).reversed())
                .collect(Collectors.toList());
    }

    @Override
    public List<TableGroup> getIncompleteTableGroups(SqlQuery query) {
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
        TableGroup tableGroup = new TableGroup().setTaskId(mappingId);
        return operationTemplate.queryCount(new QueryConfig<>(tableGroup, GroupStrategyEnum.TABLE));
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
        operationTemplate.execute(new OperationConfig(meta, CommandEnum.OPR_ADD));
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

    private void removeTableGroupDetailMeta(String tableGroupId) {
        if (StringUtil.isBlank(tableGroupId)) {
            return;
        }
        Meta byRef = metaProfile.getMetaByTaskId(tableGroupId, TaskLevelEnum.TASK_DETAIL);
        if (byRef != null && StringUtil.isNotBlank(byRef.getId())) {
            operationTemplate.remove(new OperationConfig(byRef.getId()));
        }
    }


}
