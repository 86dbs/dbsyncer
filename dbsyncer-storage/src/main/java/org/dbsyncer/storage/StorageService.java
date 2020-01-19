package org.dbsyncer.storage;

import org.dbsyncer.storage.query.Query;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/1 15:18
 */
public interface StorageService {

    List<Map> queryConfig(Query query);

    void add(String type, Map params);

    void add(String type, Map params, String collectionId);

    void edit(String type, Map params);

    void edit(String type, Map params, String collectionId);

    void remove(String type, String id);

    void remove(String type, String id, String collectionId);

}
