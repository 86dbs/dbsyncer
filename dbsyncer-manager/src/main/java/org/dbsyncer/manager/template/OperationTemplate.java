package org.dbsyncer.manager.template;

import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.GroupStrategy;
import org.dbsyncer.manager.ManagerException;
import org.dbsyncer.manager.command.PersistenceCommand;
import org.dbsyncer.manager.enums.CommandEnum;
import org.dbsyncer.manager.enums.GroupStrategyEnum;
import org.dbsyncer.manager.model.OperationConfig;
import org.dbsyncer.manager.model.QueryConfig;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.enums.StorageEnum;
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
public final class OperationTemplate {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private StorageService storageService;

    @Autowired
    private CacheService cacheService;

    public <T> List<T> queryAll(Class<T> valueType) {
        try {
            ConfigModel configModel = (ConfigModel) valueType.newInstance();
            return queryAll(new QueryConfig<T>(configModel));
        } catch (Exception e) {
            throw new ManagerException(e);
        }
    }

    public <T> List<T> queryAll(QueryConfig<T> query) {
        ConfigModel model = query.getConfigModel();
        String groupId = getGroupId(model, query.getGroupStrategyEnum());
        Group group = cacheService.get(groupId, Group.class);
        if (null != group) {
            List<String> index = group.getIndex();
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

    public int queryCount(QueryConfig query) {
        ConfigModel model = query.getConfigModel();
        String groupId = getGroupId(model, query.getGroupStrategyEnum());
        Group group = cacheService.get(groupId, Group.class);
        return null != group ? group.getIndex().size() : 0;
    }

    public <T> T queryObject(Class<T> clazz, String id) {
        if (StringUtil.isBlank(id)) {
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
        logger.debug("params:{}", params);
        CommandEnum cmd = config.getCommandEnum();
        Assert.notNull(cmd, "CommandEnum can not be null.");
        cmd.getCommandExecutor().execute(new PersistenceCommand(storageService, params));

        // 3、缓存
        cache(model, config.getGroupStrategyEnum());
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
        logger.debug("Put the model [{}] for {} group into cache.", id, groupId);
    }

    public void remove(OperationConfig config) {
        String id = config.getId();
        Assert.hasText(id, "ID can not be empty.");
        // 删除分组
        ConfigModel model = cacheService.get(id, ConfigModel.class);
        String groupId = getGroupId(model, config.getGroupStrategyEnum());
        Group group = cacheService.get(groupId, Group.class);
        if (null != group) {
            group.remove(id);
            if (0 >= group.size()) {
                cacheService.remove(groupId);
            }
        }
        cacheService.remove(id);
        storageService.remove(StorageEnum.CONFIG, id);
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

    static class Group {

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

        public int size() {
            return index.size();
        }

        public List<String> getIndex() {
            return Collections.unmodifiableList(index);
        }

        public void setIndex(List<String> index) {
            this.index = index;
        }
    }

}