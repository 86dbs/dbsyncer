/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ConnectorProfile;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.UserProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.enums.GroupStrategyEnum;
import org.dbsyncer.parser.model.Group;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.OperationConfig;
import org.dbsyncer.parser.model.QueryConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.parser.strategy.GroupStrategy;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.SortEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 通用配置存储模板（不含 User / Connector 领域特例；见 {@link UserProfile} / {@link ConnectorProfile}）。
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
@Component
public final class OperationTemplate {

    @Resource
    private StorageService storageService;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private UserProfile userProfile;

    @Resource
    private ConnectorProfile connectorProfile;

    public <T> List<T> queryAll(Class<T> valueType) {
        try {
            ConfigModel configModel = (ConfigModel) valueType.newInstance();
            StorageEnum type = ConfigModelUtil.getStorageEnum(configModel.getType());
            // task 表混存多类任务，按 TYPE 过滤
            Query condition = null;
            if (type == StorageEnum.TASK && StringUtil.isNotBlank(configModel.getType())) {
                condition = new Query();
                condition.addFilter(ConfigConstant.CONFIG_MODEL_TYPE, configModel.getType());
                condition.addOrderBy(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, SortEnum.DESC);
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
                condition.addFilter(ConfigConstant.TABLE_GROUP_TASK_ID, mappingId);
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
                condition.addFilter(ConfigConstant.TABLE_GROUP_TASK_ID, mappingId);
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
        return parseRow(data.get(0), clazz);
    }

    public String execute(OperationConfig config) {
        ConfigModel model = config.getModel();
        Assert.notNull(model, "ConfigModel can not be null.");
        CommandEnum cmd = config.getCommandEnum();
        Assert.notNull(cmd, "CommandEnum can not be null.");
        Assert.isTrue(!(model instanceof UserConfig), "UserConfig must go through UserProfile.syncUserConfig");
        if (CommandEnum.OPR_ADD == cmd) {
            if (StringUtil.isBlank(model.getId())) {
                model.setId(String.valueOf(snowflakeIdWorker.nextId()));
            }
        }

        Map<String, Object> params = ConfigModelUtil.convertModelToMap(model);
        StorageEnum type = ConfigModelUtil.getStorageEnum(model.getType());
        if (CommandEnum.OPR_EDIT == cmd) {
            storageService.edit(type, params);
        } else {
            storageService.add(type, params);
        }
        return model.getId();
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
            if (StringUtil.isBlank(model.getId())) {
                model.setId(String.valueOf(snowflakeIdWorker.nextId()));
            }
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
     * 按存储类型统计行数（仅 total，不拉明细），用于导出体积粗估。
     *
     * @param type      存储类型
     * @param condition 可选过滤条件（可为 null）
     * @return 行数
     */
    public int count(StorageEnum type, Query condition) {
        Query query = new Query();
        query.setType(type);
        query.setQueryTotal(true);
        query.setPageNum(1);
        query.setPageSize(1);
        if (condition != null) {
            query.setBooleanFilter(condition.getBooleanFilter());
        }
        Paging paging = storageService.query(query);
        return paging == null ? 0 : (int) paging.getTotal();
    }

    /**
     * 构建导出配置快照(直查库)，结构与导入 reload 保持一致：
     * type -> Group(index)、id -> model、tableGroup_{mappingId} -> Group。
     * table_group 一次全表扫描后按 taskId 分组，避免按 mapping N+1 查询。
     *
     * @return 导出快照
     */
    public Map<String, Object> buildExportSnapshot() {
        Map<String, Object> snapshot = new HashMap<>();
        List<Mapping> allMappings = queryAll(Mapping.class);
        UserConfig userConfig = userProfile.getUserConfig();

        Map<String, List<? extends ConfigModel>> typedModels = new LinkedHashMap<String, List<? extends ConfigModel>>() {{
            put(ConfigConstant.SYSTEM, queryAll(org.dbsyncer.parser.model.SystemConfig.class));
            put(ConfigConstant.USER, userConfig == null ? Collections.emptyList() : Collections.singletonList(userConfig));
            put(ConfigConstant.CONNECTOR, connectorProfile.getConnectorAll());
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

        List<TableGroup> allGroups = queryList(StorageEnum.TABLE_GROUP, null, TableGroup.class);
        Map<String, Group> groupsByTaskId = new HashMap<>();
        for (TableGroup tg : allGroups) {
            if (tg == null || StringUtil.isBlank(tg.getTaskId())) {
                continue;
            }
            snapshot.put(tg.getId(), tg);
            groupsByTaskId.computeIfAbsent(tg.getTaskId(), id -> new Group()).add(tg.getId());
        }
        allMappings.forEach(mapping -> {
            Group idGroup = groupsByTaskId.getOrDefault(mapping.getId(), new Group());
            snapshot.put(getGroupId(mapping, GroupStrategyEnum.PRELOAD_TABLE_GROUP), idGroup);
        });
        return snapshot;
    }

    /**
     * 分页查询指定存储表，反序列化 json 列为模型。
     */
    public <T> List<T> queryList(StorageEnum type, Query condition, Class<T> clazz) {
        List<T> result = new ArrayList<>();
        Query query = new Query();
        query.setType(type);
        query.setPageSize(ConfigConstant.PAGE_SIZE);
        if (condition != null) {
            query.setBooleanFilter(condition.getBooleanFilter());
        }
        while (true) {
            Paging paging = storageService.query(query);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            List<Map> data = (List<Map>) paging.getData();
            for (Map row : data) {
                T model = parseRow(row, clazz);
                if (model != null) {
                    result.add(model);
                }
            }
            query.setPageNum(query.getPageNum() + 1);
        }
        return result;
    }

    private String newInstanceType(Class<?> clazz) {
        try {
            ConfigModel model = (ConfigModel) clazz.newInstance();
            return model.getType();
        } catch (Exception e) {
            throw new ParserException(e);
        }
    }

    /**
     * 存储行 → 模型（通用路径；Connector 请走 {@link ConnectorProfile}）。
     */
    private <T> T parseRow(Map row, Class<T> clazz) {
        return ConfigModelUtil.parseFromRow(row, clazz);
    }
}
