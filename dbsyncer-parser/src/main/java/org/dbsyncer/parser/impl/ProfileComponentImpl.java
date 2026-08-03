/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ConnectorProfile;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.SystemConfigProfile;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.UserProfile;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Group;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.enums.QuartzFilterEnum;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link ProfileComponent} 门面：各领域 Profile 委托 + 导出快照编排。
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-13 21:16
 */
@Component
public class ProfileComponentImpl implements ProfileComponent {

    @Resource
    private UserProfile userProfile;

    @Resource
    private ConnectorProfile connectorProfile;

    @Resource
    private TaskProfile taskProfile;

    @Resource
    private SystemConfigProfile systemConfigProfile;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private TableGroupProfile tableGroupProfile;

    @Override
    public Connector parseConnector(String json) {
        return connectorProfile.parseConnector(json);
    }

    @Override
    public <T> T parseObject(String json, Class<T> clazz) {
        return JsonUtil.jsonToObj(json, clazz);
    }

    @Override
    public String addConfigModel(ConfigModel model) {
        if (model instanceof UserConfig) {
            return userProfile.syncUserConfig((UserConfig) model);
        }
        if (model instanceof SystemConfig) {
            return systemConfigProfile.saveSystemConfig((SystemConfig) model);
        }
        if (model instanceof Connector) {
            return connectorProfile.addConnector((Connector) model);
        }
        if (model instanceof Meta) {
            return metaProfile.addMeta((Meta) model);
        }
        if (model instanceof TableGroup) {
            return tableGroupProfile.addTableGroup((TableGroup) model);
        }
        if (StorageEnum.TASK == ConfigModelUtil.getStorageEnum(model.getType())) {
            return taskProfile.addTask(model);
        }
        throw new ParserException("Unsupported config type for add: " + model.getType());
    }

    @Override
    public String editConfigModel(ConfigModel model) {
        if (model instanceof UserConfig) {
            return userProfile.syncUserConfig((UserConfig) model);
        }
        if (model instanceof SystemConfig) {
            return systemConfigProfile.saveSystemConfig((SystemConfig) model);
        }
        if (model instanceof Connector) {
            return connectorProfile.updateConnector((Connector) model);
        }
        if (model instanceof Meta) {
            return metaProfile.updateMeta((Meta) model);
        }
        if (model instanceof TableGroup) {
            return tableGroupProfile.editTableGroup((TableGroup) model);
        }
        if (StorageEnum.TASK == ConfigModelUtil.getStorageEnum(model.getType())) {
            return taskProfile.updateTask(model);
        }
        throw new ParserException("Unsupported config type for edit: " + model.getType());
    }

    @Override
    public void removeConfigModel(String id) {
        if (StringUtil.isBlank(id)) {
            return;
        }
        if (connectorProfile.getConnector(id) != null) {
            connectorProfile.removeConnector(id);
            return;
        }
        if (metaProfile.getMeta(id) != null) {
            metaProfile.removeMeta(id);
            return;
        }
        if (tableGroupProfile.getTableGroup(id) != null) {
            tableGroupProfile.removeTableGroup(id);
            return;
        }
        if (taskProfile.existsTask(id)) {
            taskProfile.deleteTask(id);
            return;
        }
        SystemConfig systemConfig = systemConfigProfile.getSystemConfig();
        if (systemConfig != null && id.equals(systemConfig.getId())) {
            systemConfigProfile.removeSystemConfig(id);
            return;
        }
        if (userProfile.existsUser(id)) {
            userProfile.removeUser(id);
            return;
        }
        throw new ParserException("Unknown config id: " + id);
    }

    @Override
    public SystemConfig getSystemConfig() {
        return systemConfigProfile.getSystemConfig();
    }

    @Override
    public UserConfig getUserConfig() {
        return userProfile.getUserConfig();
    }

    @Override
    public Connector getConnector(String connectorId) {
        return connectorProfile.getConnector(connectorId);
    }

    @Override
    public List<Connector> getConnectorAll() {
        return connectorProfile.getConnectorAll();
    }

    @Override
    public Mapping getMapping(String mappingId) {
        return taskProfile.getTask(mappingId, Mapping.class);
    }

    @Override
    public List<Mapping> getMappingAll() {
        return taskProfile.listTasks(Mapping.class);
    }

    @Override
    public Map<String, Object> getConfigSnapshot() {
        Map<String, Object> snapshot = new HashMap<>();
        List<Mapping> allMappings = taskProfile.listTasks(Mapping.class);
        UserConfig userConfig = userProfile.getUserConfig();
        SystemConfig systemConfig = systemConfigProfile.getSystemConfig();

        Map<String, List<? extends ConfigModel>> typedModels = new LinkedHashMap<String, List<? extends ConfigModel>>() {{
            put(ConfigConstant.SYSTEM, systemConfig == null ? Collections.emptyList() : Collections.singletonList(systemConfig));
            put(ConfigConstant.USER, userConfig == null ? Collections.emptyList() : Collections.singletonList(userConfig));
            put(ConfigConstant.CONNECTOR, connectorProfile.getConnectorAll());
            put(ConfigConstant.MAPPING, allMappings);
            put(ConfigConstant.META, metaProfile.getMetaAll());
        }};

        typedModels.forEach((k, list) -> {
            Group g = new Group();
            list.forEach(m -> {
                snapshot.put(m.getId(), m);
                g.add(m.getId());
            });
            snapshot.put(k, g);
        });

        List<TableGroup> allGroups = tableGroupProfile.listTableGroupAll();
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
            snapshot.put(tableGroupProfile.getPreloadGroupKey(mapping.getId()), idGroup);
        });
        return snapshot;
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
