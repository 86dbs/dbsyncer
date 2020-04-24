package org.dbsyncer.manager.template.impl;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.manager.ManagerException;
import org.dbsyncer.manager.config.OperationCallBack;
import org.dbsyncer.manager.config.OperationConfig;
import org.dbsyncer.manager.config.QueryConfig;
import org.dbsyncer.manager.enums.GroupStrategyEnum;
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

import java.util.*;

/**
 * 操作配置模板
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
@Component
public final class OperationTemplate extends AbstractTemplate {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private StorageService storageService;

    @Autowired
    private CacheService cacheService;

    public <T> List<T> queryAll(QueryConfig<T> query) {
        ConfigModel model = query.getConfigModel();
        String groupId = getGroupId(model, getDefaultStrategy(query));
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
        if (StringUtils.isBlank(id)) {
            return null;
        }
        Object o = cacheService.get(id, clazz);
        return beanCopy(clazz, o);
    }

    public String execute(OperationConfig config) {
        // 1、解析配置
        ConfigModel model = config.getModel();
        Assert.notNull(model, "ConfigModel can not be null.");

        // 2、持久化
        Map<String, Object> params = ConfigModelUtil.convertModelToMap(model);
        logger.info("params:{}", params);
        Handler handler = config.getHandler();
        Assert.notNull(handler, "Handler can not be null.");
        handler.execute(new OperationCallBack(storageService, StorageConstant.CONFIG, params));

        // 3、缓存
        GroupStrategyEnum strategy = getDefaultStrategy(config);
        cache(model, strategy);
        return model.getId();
    }

    public void cache(ConfigModel model, GroupStrategyEnum strategy) {
        // 1、缓存
        Assert.notNull(model, "ConfigModel can not be null.");
        String id = model.getId();
        cacheService.put(id, model);

        // 2、分组
        String groupId = getGroupId(model, strategy);
        cacheService.putIfAbsent(groupId, new Group());
        Group group = cacheService.get(groupId, Group.class);
        group.addIfAbsent(id);
        logger.info("Put the model [{}] for {} group into cache.", id, groupId);
    }

    public void remove(OperationConfig config) {
        String id = config.getId();
        Assert.hasText(id, "ID can not be empty.");
        // 删除分组
        ConfigModel model = cacheService.get(id, ConfigModel.class);
        String groupId = getGroupId(model, getDefaultStrategy(config));
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

    private String getGroupId(ConfigModel model, GroupStrategyEnum strategy) {
        Assert.notNull(model, "ConfigModel can not be null.");
        Assert.notNull(strategy, "GroupStrategyEnum can not be null.");
        GroupStrategy groupStrategy = strategy.getGroupStrategy();
        Assert.notNull(groupStrategy, "GroupStrategy can not be null.");

        String groupId = groupStrategy.getGroupId(model);
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

    class Group {

        private List<String> index;

        public Group() {
            this.index = new LinkedList<>();
        }

        public synchronized void addIfAbsent(String e) {
            if (!index.contains(e)) {
                index.add(e);
            }
        }

        public synchronized void remove(String e) {
            index.remove(e);
        }

        public List<String> subList(int fromIndex, int toIndex) {
            return index.subList(fromIndex, toIndex);
        }

        public int size() {
            return index.size();
        }

        public List<String> getAll() {
            return Collections.unmodifiableList(index);
        }

    }

}