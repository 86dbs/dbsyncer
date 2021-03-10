package org.dbsyncer.storage;

import org.dbsyncer.common.model.Paging;
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

    Paging query(Query query);

    void add(StorageEnum type, Map params);

    void add(StorageEnum type, Map params, String collectionId);

    void edit(StorageEnum type, Map params);

    void edit(StorageEnum type, Map params, String collectionId);

    void remove(StorageEnum type, String id);

    void remove(StorageEnum type, String id, String collectionId);

    /**
     * 记录日志
     *
     * @param log
     * @param params
     */
    void addLog(StorageEnum log, Map<String,Object> params);

    /**
     * 记录数据
     *
     * @param data
     * @param collectionId
     * @param list
     */
    void addData(StorageEnum data, String collectionId, List<Map> list);

    /**
     * 清空数据/日志
     *
     * @param type
     * @param collectionId
     */
    void clear(StorageEnum type, String collectionId);
}