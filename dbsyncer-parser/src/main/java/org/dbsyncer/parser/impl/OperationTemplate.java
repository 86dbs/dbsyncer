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
import org.dbsyncer.parser.strategy.GroupStrategy;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.storage.StorageService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    private ConnectorFactory connectorFactory;

    public <T> List<T> queryAll(Class<T> valueType) {
        try {
            ConfigModel configModel = (ConfigModel) valueType.newInstance();
            StorageEnum type = ConfigModelUtil.getStorageEnum(configModel.getType());
            return queryList(type, null, valueType);
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
            String mappingId = ((TableGroup) model).getMappingId();
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
            String mappingId = ((TableGroup) model).getMappingId();
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

        Map<String, Object> params = ConfigModelUtil.convertModelToMap(model);
        StorageEnum type = ConfigModelUtil.getStorageEnum(model.getType());
        if (CommandEnum.OPR_EDIT == cmd) {
            // Meta 的 success/fail 为增量计数列，只能通过原子增量维护，编辑时保留库中当前值，避免被内存态覆盖
            if (model instanceof Meta) {
                preserveMetaCounters((Meta) model, params);
            }
            storageService.edit(type, params);
        } else {
            storageService.add(type, params);
        }
        return model.getId();
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
     * 编辑 Meta 时保留库中 success/fail 计数(增量列)，仅允许 total/state/snapshot 等随模型更新。
     */
    private void preserveMetaCounters(Meta model, Map<String, Object> params) {
        Meta current = queryObject(Meta.class, model.getId());
        if (current == null) {
            return;
        }
        params.put(ConfigConstant.META_SUCCESS, current.getSuccess() == null ? 0L : current.getSuccess().get());
        params.put(ConfigConstant.META_FAIL, current.getFail() == null ? 0L : current.getFail().get());
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
        storageService.remove(StorageEnum.MAPPING, id);
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
     * 从存储行反序列化模型：优先使用 json 列，Meta 计数以拆分列为准。
     */
    private <T> T deserialize(Map row, Class<T> clazz) {
        Object json = row.get(ConfigConstant.CONFIG_MODEL_JSON);
        if (json == null) {
            return null;
        }
        // 连接器配置为抽象类型 ConnectorConfig，需按 connectorType 还原具体实现类后再反序列化
        if (Connector.class.equals(clazz)) {
            return (T) parseConnector(String.valueOf(json));
        }
        T model = JsonUtil.jsonToObj(String.valueOf(json), clazz);
        if (model instanceof Meta) {
            Meta meta = (Meta) model;
            meta.setState((int) toLong(row.get(ConfigConstant.META_STATE)));
            meta.setTotal(new AtomicLong(toLong(row.get(ConfigConstant.META_TOTAL))));
            meta.setSuccess(new AtomicLong(toLong(row.get(ConfigConstant.META_SUCCESS))));
            meta.setFail(new AtomicLong(toLong(row.get(ConfigConstant.META_FAIL))));
        }
        return model;
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
}
