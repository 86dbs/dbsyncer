/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.parser.ConnectorProfile;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.UserProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.OperationConfig;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.enums.QuartzFilterEnum;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * {@link ProfileComponent} 门面：User/Connector 委托独立 Profile，其余走通用存储模板。
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-13 21:16
 */
@Component
public class ProfileComponentImpl implements ProfileComponent {

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private UserProfile userProfile;

    @Resource
    private ConnectorProfile connectorProfile;

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
        return operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD));
    }

    @Override
    public String editConfigModel(ConfigModel model) {
        if (model instanceof UserConfig) {
            return userProfile.syncUserConfig((UserConfig) model);
        }
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
        return operationTemplate.queryObject(Mapping.class, mappingId);
    }

    @Override
    public List<Mapping> getMappingAll() {
        return operationTemplate.queryAll(Mapping.class);
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
