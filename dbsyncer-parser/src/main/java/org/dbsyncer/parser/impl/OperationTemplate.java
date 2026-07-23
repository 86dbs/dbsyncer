/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.TaskSplitUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.enums.GroupStrategyEnum;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Group;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.OperationConfig;
import org.dbsyncer.parser.model.QueryConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.parser.model.UserInfo;
import org.dbsyncer.parser.strategy.GroupStrategy;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 操作配置模板（严格走库，去除内存全量缓存）
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
@Component
public final class OperationTemplate {

    /**
     * 单次分页查询的页大小
     */
    private static final int PAGE_SIZE = 1000;

    @Resource
    private StorageService storageService;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private ConnectorFactory connectorFactory;

    public <T> List<T> queryAll(Class<T> valueType) {
        try {
            if (UserConfig.class.equals(valueType)) {
                return buildUserConfigList();
            }
            ConfigModel configModel = (ConfigModel) valueType.newInstance();
            StorageEnum type = ConfigModelUtil.getStorageEnum(configModel.getType());
            // task 表混存多类任务，按 TYPE 过滤
            Query condition = null;
            if (type == StorageEnum.TASK && StringUtil.isNotBlank(configModel.getType())) {
                condition = new Query();
                condition.addFilter(ConfigConstant.CONFIG_MODEL_TYPE, configModel.getType());
            }
            return queryList(type, condition, valueType);
        } catch (Exception e) {
            throw new ParserException(e);
        }
    }

    public <T> List<T> queryAll(QueryConfig<T> query) {
        ConfigModel model = query.getConfigModel();
        StorageEnum type = ConfigModelUtil.getStorageEnum(model.getType());
        Query condition = null;
        // 表映射关系按 mappingId 过滤
        if (model instanceof TableGroup) {
            String mappingId = ((TableGroup) model).getTaskId();
            if (StringUtil.isNotBlank(mappingId)) {
                condition = new Query();
                condition.addFilter(ConfigConstant.TABLE_GROUP_MAPPING_ID, mappingId);
            }
        }
        return queryList(type, condition, (Class<T>) model.getClass());
    }

    public int queryCount(QueryConfig query) {
        ConfigModel model = query.getConfigModel();
        StorageEnum type = ConfigModelUtil.getStorageEnum(model.getType());
        Query condition = new Query();
        condition.setType(type);
        condition.setQueryTotal(true);
        if (model instanceof TableGroup) {
            String mappingId = ((TableGroup) model).getTaskId();
            if (StringUtil.isNotBlank(mappingId)) {
                condition.addFilter(ConfigConstant.TABLE_GROUP_MAPPING_ID, mappingId);
            }
        }
        Paging paging = storageService.query(condition);
        return (int) paging.getTotal();
    }

    /**
     * 按任务 ID 删除tableGroup
     */
    public void removeTableGroupsByTaskId(String taskId) {
        if (StringUtil.isBlank(taskId)) {
            return;
        }
        List<String> groupIds = listTableGroupIds(taskId);
        removeDetailMetasByTableGroupIds(groupIds);
        Query deleteQuery = new Query();
        deleteQuery.setType(StorageEnum.TABLE_GROUP);
        deleteQuery.addFilter(ConfigConstant.TABLE_GROUP_TASK_ID, taskId);
        storageService.delete(deleteQuery);
    }

    public <T> T queryObject(Class<T> clazz, String id) {
        if (StringUtil.isBlank(id)) {
            return null;
        }
        StorageEnum type = ConfigModelUtil.getStorageEnum(newInstanceType(clazz));
        Query query = new Query();
        query.setType(type);
        query.setPageNum(1);
        query.setPageSize(1);
        query.addFilter(ConfigConstant.CONFIG_MODEL_ID, id);
        Paging paging = storageService.query(query);
        List<Map> data = (List<Map>) paging.getData();
        if (CollectionUtils.isEmpty(data)) {
            return null;
        }
        return parseRow(data.get(0), clazz);
    }

    public String execute(OperationConfig config) {
        ConfigModel model = config.getModel();
        Assert.notNull(model, "ConfigModel can not be null.");
        CommandEnum cmd = config.getCommandEnum();
        Assert.notNull(cmd, "CommandEnum can not be null.");

        if (model instanceof UserConfig) {
            return syncUserConfig((UserConfig) model, cmd);
        }

        Map<String, Object> params = ConfigModelUtil.convertModelToMap(model);
        StorageEnum type = ConfigModelUtil.getStorageEnum(model.getType());
        if (CommandEnum.OPR_EDIT == cmd) {
            // 任务级 Meta：success/fail/diff/fixed 靠 increment 维护，edit 时保留库值防内存覆盖；
            // 明细级 Meta：校验/迁移直接全量更新指标，不做保护。
            if (model instanceof Meta && !((Meta) model).isTaskDetail()) {
                preserveMetaCounters((Meta) model, params);
            }
            storageService.edit(type, params);
        } else {
            storageService.add(type, params);
        }
        return model.getId();
    }

    /**
     * 按关联 ID 查询 Meta。
     */
    public Meta getMetaByTaskId(String taskId, int isTaskDetail) {
        if (StringUtil.isBlank(taskId)) {
            return null;
        }
        Query query = new Query(1, 1);
        query.setType(StorageEnum.META);
        query.addFilter(ConfigConstant.META_TASK_ID, taskId);
        query.addFilter(ConfigConstant.META_IS_TASK_DETAIL, isTaskDetail);
        Paging paging = storageService.query(query);
        if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
            return null;
        }
        Object row = paging.getData().iterator().next();
        if (row instanceof Map) {
            return ConfigModelUtil.parseFromRow((Map) row, Meta.class);
        }
        return null;
    }

    /**
     * 批量查询明细级 Meta，key 为 META.TASK_ID。
     *
     * @param refIds detailId / tableGroupId 集合
     * @return meta 映射
     */
    public Map<String, Meta> queryDetailMetaMap(List<String> refIds) {
        Map<String, Meta> result = new java.util.HashMap<>();
        if (CollectionUtils.isEmpty(refIds)) {
            return result;
        }
        List<String> ids = refIds.stream().filter(StringUtil::isNotBlank).distinct().collect(Collectors.toList());
        if (ids.isEmpty()) {
            return result;
        }
        // 分批 IN 查询，避免过长
        int batchSize = 200;
        for (int i = 0; i < ids.size(); i += batchSize) {
            List<String> batch = ids.subList(i, Math.min(i + batchSize, ids.size()));
            Query query = new Query(1, Math.max(batch.size(), 1));
            query.setType(StorageEnum.META);
            query.addFilter(ConfigConstant.META_IS_TASK_DETAIL, 1);
            query.addFilter(ConfigConstant.META_TASK_ID, org.dbsyncer.sdk.enums.FilterEnum.IN, String.join(StringUtil.COMMA, batch));
            Paging paging = storageService.query(query);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                continue;
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
        }
        return result;
    }

    /**
     * Meta 计数原子增量(严格走库)：success/fail/total 直接落库自增，避免内存 AtomicLong 累加。
     *
     * @param metaId       任务ID
     * @param totalDelta   总数增量
     * @param successDelta 成功数增量
     * @param failDelta    失败数增量
     */
    public void incrementMeta(String metaId, long totalDelta, long successDelta, long failDelta) {
        if (StringUtil.isBlank(metaId)) {
            return;
        }
        Map<String, Long> deltas = new java.util.HashMap<>();
        if (totalDelta != 0) {
            deltas.put(ConfigConstant.META_TOTAL, totalDelta);
        }
        if (successDelta != 0) {
            deltas.put(ConfigConstant.META_SUCCESS, successDelta);
        }
        if (failDelta != 0) {
            deltas.put(ConfigConstant.META_FAIL, failDelta);
        }
        if (deltas.isEmpty()) {
            return;
        }
        storageService.increment(StorageEnum.META, metaId, deltas);
    }

    /**
     * 编辑任务级 Meta 时保留库中 success/fail/diff/fixed（增量列），仅允许 total/state/snapshot 等随模型更新。
     */
    private void preserveMetaCounters(Meta model, Map<String, Object> params) {
        Meta current = queryObject(Meta.class, model.getId());
        if (current == null) {
            return;
        }
        params.put(ConfigConstant.META_SUCCESS, current.getSuccess() == null ? 0L : current.getSuccess().get());
        params.put(ConfigConstant.META_FAIL, current.getFail() == null ? 0L : current.getFail().get());
        params.put(ConfigConstant.META_DIFF, current.getDiff() == null ? 0L : current.getDiff().get());
        params.put(ConfigConstant.META_FIXED, current.getFixed() == null ? 0L : current.getFixed().get());
    }
    
    /**
     * 按 table_group.id 批量删除明细级 Meta。
     */
    public void removeDetailMetasByTableGroupIds(List<String> tableGroupIds) {
        if (CollectionUtils.isEmpty(tableGroupIds)) {
            return;
        }
        int batchSize = 1000;
        for (int from = 0; from < tableGroupIds.size(); from += batchSize) {
            int to = Math.min(from + batchSize, tableGroupIds.size());
            List<String> batch = tableGroupIds.subList(from, to);
            Query query = new Query();
            query.setType(StorageEnum.META);
            query.addFilter(ConfigConstant.META_IS_TASK_DETAIL, 1);
            query.addFilter(ConfigConstant.META_TASK_ID, FilterEnum.IN, StringUtil.join(batch, StringUtil.COMMA));
            storageService.delete(query);
        }
    }

    /**
     * 查询任务下全部 table_group.id。
     */
    public List<String> listTableGroupIds(String taskId) {
        List<String> groupIds = new ArrayList<>();
        if (StringUtil.isBlank(taskId)) {
            return groupIds;
        }
        int pageNum = 1;
        while (true) {
            Query query = new Query(pageNum, PAGE_SIZE);
            query.setType(StorageEnum.TABLE_GROUP);
            Set<String> selectFields = new java.util.HashSet<>();
            selectFields.add(ConfigConstant.CONFIG_MODEL_ID);
            query.setSelectFlied(selectFields);
            query.addFilter(ConfigConstant.TABLE_GROUP_TASK_ID, taskId);
            Paging paging = storageService.query(query);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            for (Object item : paging.getData()) {
                if (!(item instanceof Map)) {
                    continue;
                }
                Object id = ((Map) item).get(ConfigConstant.CONFIG_MODEL_ID);
                if (id != null && StringUtil.isNotBlank(String.valueOf(id))) {
                    groupIds.add(String.valueOf(id));
                }
            }
            pageNum++;
        }
        return groupIds;
    }

    /**
     * 清空任务运行结果：按 table_group.id 删明细 Meta，再 clear TASK_DETAIL；表映射仍在时补回空明细 Meta。
     */
    public void clearTaskRunResults(String taskId) {
        if (StringUtil.isBlank(taskId)) {
            return;
        }
        List<String> groupIds = listTableGroupIds(taskId);
        removeDetailMetasByTableGroupIds(groupIds);
        storageService.clear(StorageEnum.TASK_DETAIL, taskId);
        if (CollectionUtils.isEmpty(groupIds)) {
            return;
        }
        // 表映射仍保留时补回空明细 Meta，供续跑/重跑使用
        List<Meta> metas = new ArrayList<>(groupIds.size());
        long now = System.currentTimeMillis();
        for (String groupId : groupIds) {
            Meta meta = new Meta();
            meta.setId(groupId);
            meta.setTaskId(groupId);
            meta.setIsTaskDetail(1);
            meta.setCreateTime(now);
            meta.setUpdateTime(now);
            metas.add(meta);
        }
        TaskSplitUtil.split(metas, PAGE_SIZE, (models) -> {
            executeBatch(models, CommandEnum.OPR_ADD);
        });
    }

    /**
     * 明细分表按成功标记计数（走 COUNT，不拉全量行）。
     *
     * @param taskId    任务 ID
     * @param isSuccess 0-失败/未完成 1-成功
     * @return 行数
     */
    public long countTaskDetailBySuccess(String taskId, int isSuccess) {
        if (StringUtil.isBlank(taskId)) {
            return 0L;
        }
        Query query = new Query(1, 1);
        query.setType(StorageEnum.TASK_DETAIL);
        query.setMetaId(taskId);
        query.setQueryTotal(true);
        query.addFilter(ConfigConstant.DETAIL_IS_SUCCESS, isSuccess);
        Paging paging = storageService.query(query);
        return paging == null ? 0L : paging.getTotal();
    }

    /**
     * 统计明细级 Meta 中 DIFF&gt;0 的数量（校验列表错误数，避免全量连表装配）。
     *
     * @param taskId 任务 ID
     * @return 有差异的明细数
     */
    public long countDetailMetaWithPositiveDiff(String taskId) {
        if (StringUtil.isBlank(taskId)) {
            return 0L;
        }
        List<String> detailIds = new ArrayList<>();
        int pageNum = 1;
        while (true) {
            Query query = new Query(pageNum, PAGE_SIZE);
            query.setType(StorageEnum.TASK_DETAIL);
            query.setMetaId(taskId);
            Paging paging = storageService.query(query);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            for (Object item : paging.getData()) {
                if (!(item instanceof Map)) {
                    continue;
                }
                Object id = ((Map) item).get(ConfigConstant.CONFIG_MODEL_ID);
                if (id != null && StringUtil.isNotBlank(String.valueOf(id))) {
                    detailIds.add(String.valueOf(id));
                }
            }
            if (paging.getData().size() < PAGE_SIZE) {
                break;
            }
            pageNum++;
        }
        if (CollectionUtils.isEmpty(detailIds)) {
            return 0L;
        }
        Map<String, Meta> metaMap = queryDetailMetaMap(detailIds);
        long count = 0L;
        for (Meta meta : metaMap.values()) {
            if (meta != null && meta.getDiff() != null && meta.getDiff().get() > 0L) {
                count++;
            }
        }
        return count;
    }

    /**
     * 批量添加配置：单次存储批量写入。
     */
    public List<String> executeBatch(List<? extends ConfigModel> models, CommandEnum commandEnum) {
        if (CollectionUtils.isEmpty(models)) {
            return Collections.emptyList();
        }
        Assert.notNull(commandEnum, "CommandEnum can not be null.");
        Assert.isTrue(commandEnum == CommandEnum.OPR_ADD, "Batch execute only supports OPR_ADD");

        List<Map> paramsList = new ArrayList<>(models.size());
        for (ConfigModel model : models) {
            Assert.notNull(model, "ConfigModel can not be null.");
            paramsList.add(ConfigModelUtil.convertModelToMap(model));
        }
        StorageEnum type = ConfigModelUtil.getStorageEnum(models.get(0).getType());
        storageService.addBatch(type, null, paramsList);
        return models.stream().map(ConfigModel::getId).collect(Collectors.toList());
    }

    public void remove(OperationConfig config) {
        String id = config.getId();
        Assert.hasText(id, "ID can not be empty.");
        if (GroupStrategyEnum.TABLE == config.getGroupStrategyEnum()) {
            storageService.remove(StorageEnum.TABLE_GROUP, id);
            return;
        }
        // 默认删除：id 全局唯一(雪花算法)，逐个配置表删除即可命中唯一表
        storageService.remove(StorageEnum.CONFIG, id);
        storageService.remove(StorageEnum.USER, id);
        storageService.remove(StorageEnum.CONNECTOR, id);
        storageService.remove(StorageEnum.TASK, id);
        storageService.remove(StorageEnum.META, id);
    }

    public String getGroupId(ConfigModel model, GroupStrategyEnum strategy) {
        Assert.notNull(model, "ConfigModel can not be null.");
        Assert.notNull(strategy, "GroupStrategyEnum can not be null.");
        GroupStrategy groupStrategy = strategy.getGroupStrategy();
        Assert.notNull(groupStrategy, "GroupStrategy can not be null.");

        String groupId = groupStrategy.getGroupId(model);
        Assert.hasText(groupId, "GroupId can not be empty.");
        return groupId;
    }

    /**
     * 构建导出配置快照(直查库)，结构与导入 reload 保持一致：
     * type -> Group(index)、id -> model、tableGroup_{mappingId} -> Group。
     *todo 带优化 导出为一个大的sql 文件，包括
     * @return 导出快照
     */
    public Map<String, Object> buildExportSnapshot() {
        Map<String, Object> snapshot = new HashMap<>();
        List<Mapping> allMappings = queryAll(Mapping.class);

        Map<String, List<? extends ConfigModel>> typedModels = new LinkedHashMap<String, List<? extends ConfigModel>>() {{
            put(ConfigConstant.SYSTEM, queryAll(org.dbsyncer.parser.model.SystemConfig.class));
            put(ConfigConstant.USER, queryAll(org.dbsyncer.parser.model.UserConfig.class));
            put(ConfigConstant.CONNECTOR, queryAll(org.dbsyncer.parser.model.Connector.class));
            put(ConfigConstant.MAPPING, allMappings);
            put(ConfigConstant.META, queryAll(Meta.class));
        }};

        typedModels.forEach((k, list) -> {
            Group g = new Group();
            list.forEach(m -> {
                snapshot.put(m.getId(), m);
                g.add(m.getId());
            });
            snapshot.put(k, g);
        });

        allMappings.forEach(mapping -> {
            Query query = new Query();
            query.addFilter(ConfigConstant.TABLE_GROUP_MAPPING_ID, mapping.getId());
            List<TableGroup> groups = queryList(StorageEnum.TABLE_GROUP, query, TableGroup.class);
            Group idGroup = new Group();
            groups.forEach(tg -> {
                snapshot.put(tg.getId(), tg);
                idGroup.add(tg.getId());
            });
            snapshot.put(getGroupId(mapping, GroupStrategyEnum.PRELOAD_TABLE_GROUP), idGroup);
        });
        return snapshot;
    }

    /**
     * 分页查询指定存储表，反序列化 json 列为模型。
     */
    public <T> List<T> queryList(StorageEnum type, Query condition, Class<T> clazz) {
        List<T> result = new ArrayList<>();
        int pageNum = 1;
        for (; ; ) {
            Query query = new Query();
            query.setType(type);
            query.setPageNum(pageNum);
            query.setPageSize(PAGE_SIZE);
            if (condition != null) {
                query.setBooleanFilter(condition.getBooleanFilter());
            }
            Paging paging = storageService.query(query);
            List<Map> data = (List<Map>) paging.getData();
            if (CollectionUtils.isEmpty(data)) {
                break;
            }
            for (Map row : data) {
                T model = parseRow(row, clazz);
                if (model != null) {
                    result.add(model);
                }
            }
            if (data.size() < PAGE_SIZE) {
                break;
            }
            pageNum++;
        }
        return result;
    }

    /**
     * 存储行 → 模型。普通配置走 {@link ConfigModelUtil#parseFromRow}；
     * Connector 因抽象 config 需按 connectorType 特殊还原。
     */
    private <T> T parseRow(Map row, Class<T> clazz) {
        if (Connector.class.equals(clazz)) {
            Object json = row.get(ConfigConstant.CONFIG_MODEL_JSON);
            return json == null ? null : (T) parseConnector(String.valueOf(json));
        }
        return ConfigModelUtil.parseFromRow(row, clazz);
    }

    /**
     * 连接器反序列化：config 为抽象 {@link ConnectorConfig}，按 connectorType 解析到具体实现类。
     *
     * @param json 连接器 JSON
     * @return 连接器模型
     */
    public Connector parseConnector(String json) {
        Map conn = JsonUtil.parseMap(json);
        Map config = (Map) conn.remove("config");
        Connector connector = JsonUtil.jsonToObj(conn.toString(), Connector.class);
        Assert.notNull(connector, "Connector can not be null.");
        if (config != null) {
            String connectorType = (String) config.get("connectorType");
            ConnectorService connectorService = connectorFactory.getConnectorService(connectorType);
            Class<ConnectorConfig> configClass = connectorService.getConfigClass();
            connector.setConfig(JsonUtil.jsonToObj(config.toString(), configClass));
        }
        return connector;
    }

    private String newInstanceType(Class<?> clazz) {
        try {
            ConfigModel model = (ConfigModel) clazz.newInstance();
            return model.getType();
        } catch (Exception e) {
            throw new ParserException(e);
        }
    }

    private <T> List<T> buildUserConfigList() {
        List<UserInfo> users = queryAllUserInfos();
        if (CollectionUtils.isEmpty(users)) {
            return Collections.emptyList();
        }
        UserConfig config = new UserConfig();
        config.setName("用户配置");
        config.setUserInfoList(users);
        return Collections.singletonList((T) config);
    }

    private List<UserInfo> queryAllUserInfos() {
        return queryList(StorageEnum.USER, null, UserInfo.class);
    }

    /**
     * 用户配置落库：一行一用户，按账号同步增删改。
     */
    private String syncUserConfig(UserConfig config, CommandEnum cmd) {
        List<UserInfo> users = config.getUserInfoList();
        if (CollectionUtils.isEmpty(users)) {
            return config.getId();
        }
        long now = System.currentTimeMillis();
        Map<String, UserInfo> existingByUsername = queryAllUserInfos().stream()
                .collect(Collectors.toMap(UserInfo::getUsername, u -> u, (a, b) -> a));
        Set<String> keepUsernames = new HashSet<>();
        String firstId = null;
        for (UserInfo user : users) {
            keepUsernames.add(user.getUsername());
            UserInfo existing = existingByUsername.get(user.getUsername());
            if (existing != null) {
                user.setId(existing.getId());
                user.setCreateTime(existing.getCreateTime());
            } else if (StringUtil.isBlank(user.getId())) {
                user.setId(String.valueOf(snowflakeIdWorker.nextId()));
                user.setCreateTime(now);
            }
            user.setUpdateTime(now);
            Map<String, Object> params = ConfigModelUtil.convertUserInfoToMap(user);
            if (existing != null) {
                storageService.edit(StorageEnum.USER, params);
            } else {
                storageService.add(StorageEnum.USER, params);
            }
            if (firstId == null) {
                firstId = user.getId();
            }
        }
        for (UserInfo existing : existingByUsername.values()) {
            if (!keepUsernames.contains(existing.getUsername())) {
                storageService.remove(StorageEnum.USER, existing.getId());
            }
        }
        return firstId;
    }
}
