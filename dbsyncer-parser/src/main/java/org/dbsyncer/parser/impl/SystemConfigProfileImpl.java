/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.SystemConfigProfile;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.List;

/**
 * {@link SystemConfigProfile} 实现。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public class SystemConfigProfileImpl implements SystemConfigProfile {

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private StorageService storageService;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Override
    public SystemConfig getSystemConfig() {
        Query condition = new Query();
        condition.addFilter(ConfigConstant.CONFIG_MODEL_TYPE, ConfigConstant.SYSTEM);
        List<SystemConfig> list = operationTemplate.queryList(StorageEnum.CONFIG, condition, SystemConfig.class);
        return CollectionUtils.isEmpty(list) ? null : list.get(0);
    }

    @Override
    public String saveSystemConfig(SystemConfig config) {
        Assert.notNull(config, "SystemConfig can not be null.");
        long now = System.currentTimeMillis();
        if (config.getCreateTime() == null) {
            config.setCreateTime(now);
        }
        if (config.getUpdateTime() == null) {
            config.setUpdateTime(now);
        }
        if (StringUtil.isBlank(config.getId())) {
            config.setId(String.valueOf(snowflakeIdWorker.nextId()));
            storageService.add(StorageEnum.CONFIG, ConfigModelUtil.convertModelToMap(config));
        } else {
            storageService.edit(StorageEnum.CONFIG, ConfigModelUtil.convertModelToMap(config));
        }
        return config.getId();
    }

    @Override
    public int countSystemConfigs() {
        return operationTemplate.count(StorageEnum.CONFIG, null);
    }

    @Override
    public void removeSystemConfig(String id) {
        if (StringUtil.isBlank(id)) {
            return;
        }
        storageService.remove(StorageEnum.CONFIG, id);
    }

    @Override
    public void importFromJson(String json) {
        if (StringUtil.isBlank(json)) {
            return;
        }
        List<SystemConfig> configs = JsonUtil.jsonToArray(json, SystemConfig.class);
        if (CollectionUtils.isEmpty(configs)) {
            return;
        }
        for (SystemConfig config : configs) {
            saveSystemConfig(config);
        }
    }
}
