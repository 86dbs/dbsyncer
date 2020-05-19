package org.dbsyncer.storage;

import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.query.Query;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/1 15:18
 */
public interface StorageService {

    List<Map> query(StorageEnum type, Query query);

    List<Map> query(StorageEnum type, Query query, String collectionId);

    void add(StorageEnum type, Map params);

    void add(StorageEnum type, Map params, String collectionId);

    void edit(StorageEnum type, Map params);

    void edit(StorageEnum type, Map params, String collectionId);

    void remove(StorageEnum type, String id);

    void remove(StorageEnum type, String id, String collectionId);

}