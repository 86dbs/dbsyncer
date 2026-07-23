/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.TaskSplitUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.enums.GroupStrategyEnum;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.OperationConfig;
import org.dbsyncer.parser.model.QueryConfig;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.enums.QuartzFilterEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-13 21:16
 */
@Component
public class ProfileComponentImpl implements ProfileComponent {

    /**
     * table_group / 明细 Meta 批量写入单次上限
     */
    private static final int TABLE_GROUP_BATCH_SIZE = 1000;

    @Resource
    private OperationTemplate operationTemplate;

    @Override
    public Connector parseConnector(String json) {
        return operationTemplate.parseConnector(json);
    }

    @Override
    public <T> T parseObject(String json, Class<T> clazz) {
        return JsonUtil.jsonToObj(json, clazz);
    }

    @Override
    public String addConfigModel(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD));
    }

    @Override
    public String editConfigModel(ConfigModel model) {
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_EDIT));
    }

    @Override
    public void removeConfigModel(String id) {
        operationTemplate.remove(new OperationConfig(id));
    }

    @Override
    public SystemConfig getSystemConfig() {
        List<SystemConfig> list = operationTemplate.queryAll(SystemConfig.class);
        return CollectionUtils.isEmpty(list) ? null : list.get(0);
    }

    @Override
    public UserConfig getUserConfig() {
        List<UserConfig> list = operationTemplate.queryAll(UserConfig.class);
        return CollectionUtils.isEmpty(list) ? null : list.get(0);
    }

    @Override
    public Connector getConnector(String connectorId) {
        return operationTemplate.queryObject(Connector.class, connectorId);
    }

    @Override
    public List<Connector> getConnectorAll() {
        return operationTemplate.queryAll(Connector.class);
    }

    @Override
    public Mapping getMapping(String mappingId) {
        return operationTemplate.queryObject(Mapping.class, mappingId);
    }

    @Override
    public List<Mapping> getMappingAll() {
        return operationTemplate.queryAll(Mapping.class);
    }

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
        List<String> allIds = new ArrayList<>(models.size());
        TaskSplitUtil.split(models, TABLE_GROUP_BATCH_SIZE, batch -> {
            List<String> ids = operationTemplate.executeBatch(batch, CommandEnum.OPR_ADD);
            allIds.addAll(ids);
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
        return getTableGroupAll(mappingId).stream().sorted(Comparator.comparing(TableGroup::getIndex).reversed()).collect(Collectors.toList());
    }

    @Override
    public int getTableGroupCount(String mappingId) {
        TableGroup tableGroup = new TableGroup().setTaskId(mappingId);
        return operationTemplate.queryCount(new QueryConfig<>(tableGroup, GroupStrategyEnum.TABLE));
    }

    @Override
    public Meta getMeta(String metaId) {
        return operationTemplate.queryObject(Meta.class, metaId);
    }

    @Override
    public List<Meta> getMetaAll() {
        return operationTemplate.queryAll(Meta.class);
    }

    @Override
    public List<Meta> getTaskMetaAll() {
        Query condition = new Query();
        condition.addFilter(ConfigConstant.META_IS_TASK_DETAIL, 0);
        return operationTemplate.queryList(StorageEnum.META, condition, Meta.class);
    }

    @Override
    public Meta getMetaByTaskId(String refId, int isTaskDetail) {
        return operationTemplate.getMetaByTaskId(refId, isTaskDetail);
    }

    @Override
    public Map<String, Meta> getDetailMetaMap(List<String> refIds) {
        return operationTemplate.queryDetailMetaMap(refIds);
    }

    @Override
    public void removeDetailMetasByTaskId(String taskId) {
        operationTemplate.removeDetailMetasByTableGroupIds(operationTemplate.listTableGroupIds(taskId));
    }

    @Override
    public void clearTaskRunResults(String taskId) {
        operationTemplate.clearTaskRunResults(taskId);
    }

    @Override
    public void resetTaskMeta(String taskId) {
        if (StringUtil.isBlank(taskId)) {
            return;
        }
        Meta meta = getMetaByTaskId(taskId,0);
        if (meta == null) {
            return;
        }
        meta.clear();
        meta.setTaskId(taskId);
        meta.setIsTaskDetail(0);
        meta.setUpdateTime(System.currentTimeMillis());
        editConfigModel(meta);
    }

    /**
     * 为 table_group 预建明细 Meta（已存在则跳过）。
     */
    private void addTableGroupDetailMeta(String tableGroupId) {
        if (StringUtil.isBlank(tableGroupId)) {
            return;
        }
        Meta existing = getMetaByTaskId(tableGroupId, 1);
        if (existing != null) {
            return;
        }
        Meta byId = getMeta(tableGroupId);
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
        addConfigModel(meta);
    }

    private void removeTableGroupDetailMeta(String tableGroupId) {
        if (StringUtil.isBlank(tableGroupId)) {
            return;
        }
        Meta byRef = getMetaByTaskId(tableGroupId, 1);
        if (byRef != null && StringUtil.isNotBlank(byRef.getId())) {
            removeConfigModel(byRef.getId());
            return;
        }
        Meta byId = getMeta(tableGroupId);
        if (byId != null && byId.isTaskDetail()) {
            removeConfigModel(byId.getId());
        }
    }

    @Override
    public long countTaskDetailBySuccess(String taskId, int isSuccess) {
        return operationTemplate.countTaskDetailBySuccess(taskId, isSuccess);
    }

    @Override
    public long sumTaskDetailMetaDiff(String taskId) {
        return operationTemplate.countDetailMetaWithPositiveDiff(taskId);
    }

    @Override
    public void incrementMeta(String metaId, long totalDelta, long successDelta, long failDelta) {
        operationTemplate.incrementMeta(metaId, totalDelta, successDelta, failDelta);
    }

    @Override
    public Map<String, Object> getConfigSnapshot() {
        return operationTemplate.buildExportSnapshot();
    }

    @Override
    public List<OperationEnum> getOperationEnumAll() {
        return Arrays.asList(OperationEnum.values());
    }

    @Override
    public List<QuartzFilterEnum> getQuartzFilterEnumAll() {
        return Arrays.asList(QuartzFilterEnum.values());
    }

    @Override
    public List<FilterEnum> getFilterEnumAll() {
        return Arrays.asList(FilterEnum.values());
    }

    @Override
    public List<ConvertEnum> getConvertEnumAll() {
        return Arrays.asList(ConvertEnum.values());
    }

    @Override
    public List<StorageDataStatusEnum> getStorageDataStatusEnumAll() {
        return Arrays.asList(StorageDataStatusEnum.values());
    }

}