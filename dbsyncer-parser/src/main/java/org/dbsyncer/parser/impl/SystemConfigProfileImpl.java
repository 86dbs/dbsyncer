/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.cache.CacheConstant;
import org.dbsyncer.common.cache.CacheService;
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

    @Resource
    private CacheService cacheService;

    @Override
    public SystemConfig getSystemConfig() {
        SystemConfig cached = cacheService.get(CacheConstant.SYSTEM_CONFIG, SystemConfig.class);
        if (cached != null) {
            return cached;
        }
        return cacheService.executeWithLock(CacheConstant.SYSTEM_CONFIG_LOCK, () -> {
            SystemConfig again = cacheService.get(CacheConstant.SYSTEM_CONFIG, SystemConfig.class);
            if (again != null) {
                return again;
            }
            SystemConfig config = querySystemConfig();
            if (config != null) {
                // 配置变更低频，本地常驻；save/remove 时主动刷新
                cacheService.put(CacheConstant.SYSTEM_CONFIG, config);
            }
            return config;
        });
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
        cacheService.executeWithLock(CacheConstant.SYSTEM_CONFIG_LOCK, () -> {
            SystemConfig latest = querySystemConfig();
            if (latest != null) {
                cacheService.put(CacheConstant.SYSTEM_CONFIG, latest);
            } else {
                cacheService.remove(CacheConstant.SYSTEM_CONFIG);
            }
        });
        return config.getId();
    }

    @Override
    public int countSystemConfigs() {
        return operationTemplate.count(StorageEnum.CONFIG, null);
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

    private SystemConfig querySystemConfig() {
        Query condition = new Query();
        condition.addFilter(ConfigConstant.CONFIG_MODEL_TYPE, ConfigConstant.SYSTEM);
        List<SystemConfig> list = operationTemplate.queryList(StorageEnum.CONFIG, condition, SystemConfig.class);
        return CollectionUtils.isEmpty(list) ? null : list.get(0);
    }
}
