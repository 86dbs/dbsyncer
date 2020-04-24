package org.dbsyncer.storage;

import org.dbsyncer.storage.query.Query;
import org.dbsyncer.storage.strategy.Strategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/16 1:28
 */
public abstract class AbstractStorageService implements StorageService, ApplicationContextAware {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, Strategy> map;

    public abstract List<Map> query(String collectionId, Query query);

    public abstract void insert(String collectionId, Map params) throws IOException;

    public abstract void update(String collectionId, Map params) throws IOException;

    public abstract void delete(String collectionId, String id) throws IOException;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        map = applicationContext.getBeansOfType(Strategy.class);
    }

    @Override
    public void add(String type, Map params) {
        add(type, params, null);
    }

    @Override
    public void add(String type, Map params, String collectionId) {
        Assert.hasText(type, "Type can not be empty.");
        Assert.notNull(params, "Params can not be null.");
        logger.debug("collectionId:{}, params:{}", collectionId, params);
        try {
            insert(getCollectionId(type, collectionId), params);
        } catch (IOException e) {
            logger.error("add collectionId:{}, params:{}, failed:{}", collectionId, params, e.getMessage());
            throw new StorageException(e);
        }
    }

    @Override
    public void edit(String type, Map params) {
        edit(type, params, null);
    }

    @Override
    public void edit(String type, Map params, String collectionId) {
        Assert.notNull(params, "Params can not be null.");
        logger.debug("collectionId:{}, params:{}", collectionId, params);
        try {
            update(getCollectionId(type, collectionId), params);
        } catch (IOException e) {
            logger.error("edit collectionId:{}, params:{}, failed:{}", collectionId, params, e.getMessage());
            throw new StorageException(e);
        }
    }

    @Override
    public void remove(String type, String id) {
        remove(type, id, null);
    }

    @Override
    public void remove(String type, String id, String collectionId) {
        Assert.hasText(id, "ID can not be null.");
        logger.debug("collectionId:{}, id:{}", collectionId, id);
        try {
            delete(getCollectionId(type, collectionId), id);
        } catch (IOException e) {
            logger.error("remove collectionId:{}, id:{}, failed:{}", collectionId, id, e.getMessage());
            throw new StorageException(e);
        }
    }

    private String getCollectionId(String type, String collectionId) {
        Strategy strategy = map.get(type);
        Assert.notNull(strategy, "Type does not exist.");
        return strategy.createCollectionId(collectionId);
    }

}