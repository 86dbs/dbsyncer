package org.dbsyncer.storage;

import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.query.Query;
import org.dbsyncer.storage.strategy.Strategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.Assert;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/16 1:28
 */
public abstract class AbstractStorageService implements StorageService, ApplicationContextAware, DisposableBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, Strategy> map;

    public abstract List<Map> select(Query query) throws IOException;

    public abstract void insert(StorageEnum type, String collection, Map params) throws IOException;

    public abstract void update(StorageEnum type, String collection, Map params) throws IOException;

    public abstract void delete(StorageEnum type, String collection, String id) throws IOException;

    public abstract void deleteAll(StorageEnum type, String collection) throws IOException;

    /**
     * 记录日志
     *
     * @param collection
     * @param params
     */
    public abstract void insertLog(StorageEnum type, String collection, Map<String, Object> params) throws IOException;

    /**
     * 记录错误数据
     *
     * @param collection
     * @param list
     */
    public abstract void insertData(StorageEnum type, String collection, List<Map> list) throws IOException;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        map = applicationContext.getBeansOfType(Strategy.class);
    }

    @Override
    public List<Map> query(Query query) {
        try {
            String collection = getCollection(query.getType(), query.getCollection());
            query.setCollection(collection);
            return select(query);
        } catch (IOException e) {
            logger.error("query collectionId:{}, params:{}, failed:{}", query.getCollection(), query.getParams(), e.getMessage());
            throw new StorageException(e);
        }
    }

    @Override
    public void add(StorageEnum type, Map params) {
        add(type, params, null);
    }

    @Override
    public void add(StorageEnum type, Map params, String collectionId) {
        Assert.notNull(params, "Params can not be null.");
        logger.debug("collectionId:{}, params:{}", collectionId, params);
        try {
            String collection = getCollection(type, collectionId);
            insert(type, collection, params);
        } catch (IOException e) {
            logger.error("add collectionId:{}, params:{}, failed:{}", collectionId, params, e.getMessage());
            throw new StorageException(e);
        }
    }

    @Override
    public void edit(StorageEnum type, Map params) {
        edit(type, params, null);
    }

    @Override
    public void edit(StorageEnum type, Map params, String collectionId) {
        Assert.notNull(params, "Params can not be null.");
        logger.debug("collectionId:{}, params:{}", collectionId, params);
        try {
            String collection = getCollection(type, collectionId);
            update(type, collection, params);
        } catch (IOException e) {
            logger.error("edit collectionId:{}, params:{}, failed:{}", collectionId, params, e.getMessage());
            throw new StorageException(e);
        }
    }

    @Override
    public void remove(StorageEnum type, String id) {
        remove(type, id, null);
    }

    @Override
    public void remove(StorageEnum type, String id, String collectionId) {
        Assert.hasText(id, "ID can not be null.");
        logger.debug("collectionId:{}, id:{}", collectionId, id);
        try {
            String collection = getCollection(type, collectionId);
            delete(type, collection, id);
        } catch (IOException e) {
            logger.error("remove collectionId:{}, id:{}, failed:{}", collectionId, id, e.getMessage());
            throw new StorageException(e);
        }
    }

    @Override
    public void addLog(StorageEnum type, Map<String, Object> params) {
        try {
            String collection = getCollection(type, null);
            insertLog(type, collection, params);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new StorageException(e);
        }
    }

    @Override
    public void addData(StorageEnum type, String collectionId, List<Map> list) {
        try {
            String collection = getCollection(type, collectionId);
            insertData(type, collection, list);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new StorageException(e);
        }
    }

    @Override
    public void clear(StorageEnum type, String collectionId) {
        try {
            String collection = getCollection(type, collectionId);
            deleteAll(type, collection);
        } catch (IOException e) {
            logger.error("clear collectionId:{}, failed:{}", collectionId, e.getMessage());
            throw new StorageException(e);
        }
    }

    protected String getSeparator(){
        return File.separator;
    }

    private String getCollection(StorageEnum type, String collection) {
        Assert.notNull(type, "StorageEnum type can not be null.");
        Strategy strategy = map.get(type.getType().concat("Strategy"));
        Assert.notNull(strategy, "Strategy does not exist.");
        return strategy.createCollectionId(getSeparator(), collection);
    }

}