/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
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
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
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

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 单次分页查询的页大小
     */
    private static final int PAGE_SIZE = 100;

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
        return deserialize(data.get(0), clazz);
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
     *
     * @param refId        任务ID / detailId / tableGroupId
     * @param isTaskDetail 0-任务级 1-明细级
     * @return Meta 或 null
     */
    public Meta queryMetaByRefId(String refId, int isTaskDetail) {
        if (StringUtil.isBlank(refId)) {
            return null;
        }
        Query query = new Query(1, 1);
        query.setType(StorageEnum.META);
        query.addFilter(ConfigConstant.META_TASK_ID, refId);
        query.addFilter(ConfigConstant.META_IS_TASK_DETAIL, isTaskDetail);
        Paging paging = storageService.query(query);
        if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
            return null;
        }
        Object row = paging.getData().iterator().next();
        if (row instanceof Map) {
            return deserializeMeta((Map) row);
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
                Meta meta = deserializeMeta((Map) item);
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
     * 仅任务级 Meta（IS_TASK_DETAIL=0）。
     *
     * @return 任务级 Meta 列表
     */
    public List<Meta> queryTaskMetaAll() {
        Query condition = new Query();
        condition.addFilter(ConfigConstant.META_IS_TASK_DETAIL, 0);
        return queryList(StorageEnum.META, condition, Meta.class);
    }

    /**
     * 按任务分表中的明细 ID，删除对应明细级 Meta（IS_TASK_DETAIL=1，TASK_ID=detailId）。
     * <p>须在 {@code clear(TASK_DETAIL)} 之前调用。
     *
     * @param taskId 任务 ID（分表键）
     */
    public void removeDetailMetasByTaskId(String taskId) {
        if (StringUtil.isBlank(taskId)) {
            return;
        }
        List<String> detailIds = new ArrayList<>();
        int pageNum = 1;
        int pageSize = 500;
        while (true) {
            Query query = new Query(pageNum, pageSize);
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
            if (paging.getData().size() < pageSize) {
                break;
            }
            pageNum++;
        }
        if (CollectionUtils.isEmpty(detailIds)) {
            return;
        }
        Map<String, Meta> metaMap = queryDetailMetaMap(detailIds);
        for (Meta meta : metaMap.values()) {
            if (meta != null && StringUtil.isNotBlank(meta.getId())) {
                storageService.remove(StorageEnum.META, meta.getId());
            }
        }
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
        int pageSize = 500;
        while (true) {
            Query query = new Query(pageNum, pageSize);
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
            if (paging.getData().size() < pageSize) {
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
    public List<String> executeBatch(List<? extends ConfigModel> models, CommandEnum commandEnum,
                                     GroupStrategyEnum groupStrategyEnum) {
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
     *
     * @return 导出快照
     */
    public Map<String, Object> buildExportSnapshot() {
        Map<String, Object> snapshot = new java.util.HashMap<>();
        appendGroup(snapshot, ConfigConstant.SYSTEM, queryAll(org.dbsyncer.parser.model.SystemConfig.class));
        appendGroup(snapshot, ConfigConstant.USER, queryAll(org.dbsyncer.parser.model.UserConfig.class));
        appendGroup(snapshot, ConfigConstant.CONNECTOR, queryAll(org.dbsyncer.parser.model.Connector.class));
        List<Mapping> mappingAll = queryAll(Mapping.class);
        appendGroup(snapshot, ConfigConstant.MAPPING, mappingAll);
        appendGroup(snapshot, ConfigConstant.META, queryAll(Meta.class));

        // 表映射关系按 mapping 分组
        for (Mapping mapping : mappingAll) {
            List<TableGroup> tableGroups = queryList(StorageEnum.TABLE_GROUP, buildMappingFilter(mapping.getId()), TableGroup.class);
            String groupId = getGroupId(mapping, GroupStrategyEnum.PRELOAD_TABLE_GROUP);
            Group group = new Group();
            for (TableGroup tableGroup : tableGroups) {
                group.add(tableGroup.getId());
                snapshot.put(tableGroup.getId(), tableGroup);
            }
            snapshot.put(groupId, group);
        }
        return snapshot;
    }

    private Query buildMappingFilter(String mappingId) {
        Query condition = new Query();
        condition.addFilter(ConfigConstant.TABLE_GROUP_MAPPING_ID, mappingId);
        return condition;
    }

    private void appendGroup(Map<String, Object> snapshot, String type, List<? extends ConfigModel> models) {
        Group group = new Group();
        for (ConfigModel model : models) {
            group.add(model.getId());
            snapshot.put(model.getId(), model);
        }
        snapshot.put(type, group);
    }

    /**
     * 分页查询指定存储表，反序列化 json 列为模型。
     */
    private <T> List<T> queryList(StorageEnum type, Query condition, Class<T> clazz) {
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
                T model = deserialize(row, clazz);
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
     * 从存储行反序列化模型。Meta 无 JSON 列，按拆分列还原；其余优先 json 列。
     */
    private <T> T deserialize(Map row, Class<T> clazz) {
        if (Meta.class.equals(clazz)) {
            return (T) deserializeMeta(row);
        }
        if (UserInfo.class.equals(clazz)) {
            return (T) deserializeUserInfo(row);
        }
        Object json = row.get(ConfigConstant.CONFIG_MODEL_JSON);
        if (json == null) {
            return null;
        }
        // 连接器配置为抽象类型 ConnectorConfig，需按 connectorType 还原具体实现类后再反序列化
        if (Connector.class.equals(clazz)) {
            return (T) parseConnector(String.valueOf(json));
        }
        T model = JsonUtil.jsonToObj(String.valueOf(json), clazz);
        if (model instanceof TableGroup) {
            overlayTableGroupColumns((TableGroup) model, row);
        }
        return model;
    }

    /**
     * 按 dbsyncer_meta 拆分列还原 Meta
     */
    private Meta deserializeMeta(Map row) {
        Meta meta = new Meta();
        meta.setId(String.valueOf(row.get(ConfigConstant.CONFIG_MODEL_ID)));
        meta.setCreateTime(toLong(row.get(ConfigConstant.CONFIG_MODEL_CREATE_TIME)));
        meta.setUpdateTime(toLong(row.get(ConfigConstant.CONFIG_MODEL_UPDATE_TIME)));
        Object taskId = row.get(ConfigConstant.META_TASK_ID);
        meta.setTaskId(taskId == null ? null : String.valueOf(taskId));
        meta.setState((int) toLong(row.get(ConfigConstant.META_STATE)));
        meta.setIsTaskDetail((int) toLong(row.get(ConfigConstant.META_IS_TASK_DETAIL)));
        meta.setTotal(new AtomicLong(toLong(row.get(ConfigConstant.META_TOTAL))));
        meta.setSuccess(new AtomicLong(toLong(row.get(ConfigConstant.META_SUCCESS))));
        meta.setFail(new AtomicLong(toLong(row.get(ConfigConstant.META_FAIL))));
        meta.setDiff(new AtomicLong(toLong(row.get(ConfigConstant.META_DIFF))));
        meta.setFixed(new AtomicLong(toLong(row.get(ConfigConstant.META_FIXED))));
        Object snapshot = row.get(ConfigConstant.META_SNAPSHOT);
        if (snapshot != null && StringUtil.isNotBlank(String.valueOf(snapshot))) {
            Map map = JsonUtil.parseMap(String.valueOf(snapshot));
            if (map != null) {
                Map<String, String> snap = new java.util.HashMap<>();
                map.forEach((k, v) -> snap.put(String.valueOf(k), v == null ? null : String.valueOf(v)));
                meta.setSnapshot(snap);
            }
        }
        return meta;
    }

    /**
     * 用拆分列覆盖 TableGroup 关联字段(以列为准)
     */
    private void overlayTableGroupColumns(TableGroup tableGroup, Map row) {
        Object taskId = row.get(ConfigConstant.TABLE_GROUP_TASK_ID);
        if (taskId != null) {
            tableGroup.setTaskId(String.valueOf(taskId));
        }
        if (row.get(ConfigConstant.TABLE_GROUP_SORT_INDEX) != null) {
            tableGroup.setIndex((int) toLong(row.get(ConfigConstant.TABLE_GROUP_SORT_INDEX)));
        }
        Object srcConn = row.get(ConfigConstant.TABLE_GROUP_SOURCE_CONNECTOR_ID);
        if (srcConn != null) {
            tableGroup.setSourceConnectorId(String.valueOf(srcConn));
        }
        Object tgtConn = row.get(ConfigConstant.TABLE_GROUP_TARGET_CONNECTOR_ID);
        if (tgtConn != null) {
            tableGroup.setTargetConnectorId(String.valueOf(tgtConn));
        }
        Object srcDb = row.get(ConfigConstant.TABLE_GROUP_SOURCE_DATABASE);
        if (srcDb != null) {
            tableGroup.setSourceDatabase(String.valueOf(srcDb));
        }
        Object tgtDb = row.get(ConfigConstant.TABLE_GROUP_TARGET_DATABASE);
        if (tgtDb != null) {
            tableGroup.setTargetDatabase(String.valueOf(tgtDb));
        }
        Object srcSchema = row.get(ConfigConstant.TABLE_GROUP_SOURCE_SCHEMA);
        if (srcSchema != null) {
            tableGroup.setSourceSchema(String.valueOf(srcSchema));
        }
        Object tgtSchema = row.get(ConfigConstant.TABLE_GROUP_TARGET_SCHEMA);
        if (tgtSchema != null) {
            tableGroup.setTargetSchema(String.valueOf(tgtSchema));
        }
        tableGroup.setSourceTotal(toLong(row.get(ConfigConstant.TABLE_GROUP_SOURCE_TOTAL)));
        tableGroup.setTargetTotal(toLong(row.get(ConfigConstant.TABLE_GROUP_TARGET_TOTAL)));
    }

    private long toLong(Object value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException e) {
            return 0L;
        }
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

    @SuppressWarnings("unchecked")
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

    private UserInfo deserializeUserInfo(Map row) {
        UserInfo user = new UserInfo();
        user.setId(String.valueOf(row.get(ConfigConstant.CONFIG_MODEL_ID)));
        user.setCreateTime(toLong(row.get(ConfigConstant.CONFIG_MODEL_CREATE_TIME)));
        user.setUpdateTime(toLong(row.get(ConfigConstant.CONFIG_MODEL_UPDATE_TIME)));
        user.setUsername(String.valueOf(row.get(ConfigConstant.USER_USERNAME)));
        user.setPassword(String.valueOf(row.get(ConfigConstant.USER_PASSWORD)));
        user.setNickname(String.valueOf(row.get(ConfigConstant.USER_NICKNAME)));
        Object role = row.get(ConfigConstant.USER_ROLE);
        user.setRoleCode(role == null ? null : String.valueOf(role));
        Object email = row.get(ConfigConstant.USER_EMAIL);
        user.setEmail(email == null ? StringUtil.EMPTY : String.valueOf(email));
        Object phone = row.get(ConfigConstant.USER_PHONE);
        user.setPhone(phone == null ? StringUtil.EMPTY : String.valueOf(phone));
        return user;
    }
}
