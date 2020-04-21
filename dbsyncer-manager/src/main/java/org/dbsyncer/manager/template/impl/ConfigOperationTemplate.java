package org.dbsyncer.manager.template.impl;

import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.manager.ManagerException;
import org.dbsyncer.manager.template.*;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.constant.StorageConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 操作配置模板
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
@Component
public class ConfigOperationTemplate {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private StorageService storageService;

    @Autowired
    private CacheService cacheService;

    public <T> List<T> queryAll(QueryTemplate<T> queryTemplate) {
        ConfigModel model = queryTemplate.getConfigModel();
        String groupId = getGroupId(model, queryTemplate);
        Group group = cacheService.get(groupId, Group.class);
        if (null != group) {
            List<String> index = group.getAll();
            if (!CollectionUtils.isEmpty(index)) {
                List<T> list = new ArrayList<>();
                Class<? extends ConfigModel> clazz = model.getClass();
                index.forEach(e -> {
                    Object v = cacheService.get(e);
                    if (null != v) {
                        list.add((T) beanCopy(clazz, v));
                    }
                });
                return list;
            }
        }
        return Collections.EMPTY_LIST;
    }

    public <T> T queryObject(Class<T> clazz, String id) {
        Object o = cacheService.get(id, clazz);
        return beanCopy(clazz, o);
    }

    public String execute(ConfigModel model, OperationTemplate operationTemplate) {
        // 1、解析配置
        Assert.notNull(model, "ConfigModel can not be null.");
        String id = model.getId();
        logger.info("Parse config model id:{}", id);

        // 2、持久化
        Map<String, Object> params = ConfigModelUtil.convertModelToMap(model);
        operationTemplate.handleEvent(new Call(StorageConstant.CONFIG, params));
        logger.info("Stored config model:{}", id);

        // 3、缓存
        save(model, operationTemplate);
        return id;
    }

    public void save(ConfigModel model, BaseTemplate baseTemplate) {
        // 1、缓存
        Assert.notNull(model, "ConfigModel can not be null.");
        String id = model.getId();
        cacheService.put(id, model);
        logger.info("Save config into cache:[{}]", id);

        // 2、分组
        String groupId = getGroupId(model, baseTemplate);
        cacheService.putIfAbsent(groupId, new Group());
        Group group = cacheService.get(groupId, Group.class);
        group.addIfAbsent(id);
        logger.info("Save group into cache:[{}]", groupId);
    }

    public void remove(RemoveTemplate removeTemplate) {
        String id = removeTemplate.getId();
        Assert.hasText(id, "ID can not be empty.");
        // 删除分组
        ConfigModel model = cacheService.get(id, ConfigModel.class);
        String groupId = getGroupId(model, removeTemplate);
        Group group = cacheService.get(groupId, Group.class);
        if (null != group) {
            group.remove(id);
            if (0 >= group.size()) {
                cacheService.remove(groupId);
            }
        }
        cacheService.remove(id);
        storageService.remove(StorageConstant.CONFIG, id);
    }

    private String getGroupId(ConfigModel model, BaseTemplate template) {
        Assert.notNull(model, "ConfigModel can not be null.");
        Assert.notNull(template, "BaseTemplate can not be null.");
        GroupStrategy strategy = template.getGroupStrategy();
        Assert.notNull(strategy, "GroupStrategy can not be null.");
        String groupId = strategy.getGroupId(model);
        Assert.hasText(groupId, "GroupId can not be empty.");
        return groupId;
    }

    private <T> T beanCopy(Class<T> clazz, Object o) {
        if (null == o || null == clazz) {
            return null;
        }
        try {
            T t = clazz.newInstance();
            BeanUtils.copyProperties(o, t);
            return t;
        } catch (InstantiationException e) {
            throw new ManagerException(e.getMessage());
        } catch (IllegalAccessException e) {
            throw new ManagerException(e.getMessage());
        }
    }

    public final class Call {

        private String type;

        private Map params;

        public Call(String type, Map params) {
            this.type = type;
            this.params = params;
        }

        public void add() {
            storageService.add(type, params);
        }

        public void edit() {
            storageService.edit(type, params);
        }

    }

}