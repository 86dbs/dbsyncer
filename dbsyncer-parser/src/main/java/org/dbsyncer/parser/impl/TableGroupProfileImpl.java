/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.enums.TaskLevelEnum;
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
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link TableGroupProfile} 实现（dbsyncer_table_group）。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public class TableGroupProfileImpl implements TableGroupProfile {

    /**
     * table_group / 明细 Meta 批量写入单次上限
     */
    private static final int TABLE_GROUP_BATCH_SIZE = 1000;

    @Resource
    private OperationTemplate operationTemplate;

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
        TaskSplitUtil.split(models, TABLE_GROUP_BATCH_SIZE, batch -> {
            List<String> ids = operationTemplate.executeBatch(batch, CommandEnum.OPR_ADD);
            List<Meta> metas = new ArrayList<>(ids.size());
            long now = System.currentTimeMillis();
            for (String id : ids) {
                if (StringUtil.isBlank(id)) {
                    continue;
                }
                Meta meta = new Meta();
                meta.setId(id);
                meta.setTaskId(id);
                meta.setIsTaskDetail(1);
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
        operationTemplate.removeTableGroupsByTaskId(taskId);
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
        Meta byId = metaProfile.getMeta(tableGroupId);
        if (byId != null && byId.isTaskDetail()) {
            return;
        }
        Meta meta = new Meta();
        meta.setId(tableGroupId);
        meta.setTaskId(tableGroupId);
        meta.setIsTaskDetail(1);
        long now = System.currentTimeMillis();
        meta.setCreateTime(now);
        meta.setUpdateTime(now);
        operationTemplate.execute(new OperationConfig(meta, CommandEnum.OPR_ADD));
    }

    private void removeTableGroupDetailMeta(String tableGroupId) {
        if (StringUtil.isBlank(tableGroupId)) {
            return;
        }
        Meta byRef = metaProfile.getMetaByTaskId(tableGroupId, TaskLevelEnum.TASK_DETAIL);
        if (byRef != null && StringUtil.isNotBlank(byRef.getId())) {
            operationTemplate.remove(new OperationConfig(byRef.getId()));
            return;
        }
        Meta byId = metaProfile.getMeta(tableGroupId);
        if (byId != null && byId.isTaskDetail()) {
            operationTemplate.remove(new OperationConfig(byId.getId()));
        }
    }
}
