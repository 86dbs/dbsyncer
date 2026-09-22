/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.cache.CacheService;
import org.dbsyncer.common.message.impl.RemoveConfigModelCacheMessage;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.impl.OperationTemplate;
import org.dbsyncer.sdk.spi.ClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.lang.reflect.ParameterizedType;

/**
 * 通用配置（负责缓存刷新维护）
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-09-22 21:01
 */
public abstract class AbstractConfigModelProfile<T extends ConfigModel> implements ConfigModelProfile {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Class<ConfigModel> responseClazz;
    private String configModelType;

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private CacheService cacheService;

    @Resource
    private ClusterService clusterService;

    public AbstractConfigModelProfile() {
        int level = 5;
        Class<?> aClass = getClass();
        while (level > 0) {
            if (aClass.getSuperclass() == AbstractConfigModelProfile.class) {
                responseClazz = (Class<ConfigModel>) ((ParameterizedType) aClass.getGenericSuperclass()).getActualTypeArguments()[0];
                break;
            }
            aClass = aClass.getSuperclass();
            level--;
        }
        Assert.notNull(responseClazz, String.format("%s的父类%s泛型参数Response为空.", getClass().getName(), AbstractConfigModelProfile.class.getName()));
        try {
            configModelType = responseClazz.newInstance().getType();
        } catch (InstantiationException | IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        }
    }

    protected ConfigModel getConfigModel(String id) {
        return operationTemplate.queryObject(responseClazz, id);
    }

    @Override
    public T getCache(String id) {
        ConfigModel cached = cacheService.get(configModelType, responseClazz);
        if (cached != null) {
            return (T) cached;
        }
        String lockKey = buildLockKey(id);
        return cacheService.executeWithLock(lockKey, () -> {
            T again = (T) cacheService.get(configModelType, responseClazz);
            if (again != null) {
                return again;
            }
            T config = (T) getConfigModel(id);
            if (config != null) {
                // 配置变更低频，本地常驻；save/remove 时主动刷新
                cacheService.put(configModelType, config);
            }
            return config;
        });
    }

    @Override
    public void removeCacheAndNotice(String id) {
        cacheService.remove(buildLockKey(id));
        RemoveConfigModelCacheMessage message = new RemoveConfigModelCacheMessage();
        message.setId(id);
        message.setConfigModelType(configModelType);
        clusterService.pushMessage(message);
    }

    @Override
    public void removeCache(String id) {
        cacheService.remove(buildLockKey(id));
    }

    private String buildLockKey(String id) {
        String key = configModelType + ":lock";
        if (StringUtil.isBlank(id)) {
            return key;
        }
        return key + ":" + id;
    }

}