package org.dbsyncer.storage;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.Query;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/1 15:18
 */
public interface StorageService {

    /**
     * 查询所有数据
     *
     * @param query
     * @return
     */
    Paging query(Query query);

    /**
     * 根据条件删除
     *
     * @param query
     */
    void delete(Query query);

    /**
     * 清空数据/日志
     *
     * @param type
     * @param metaId
     */
    void clear(StorageEnum type, String metaId);

    /**
     * 添加
     *
     * @param type
     * @param params
     */
    void add(StorageEnum type, Map params);

    /**
     * 添加
     *
     * @param type
     * @param metaId
     * @param params
     */
    void add(StorageEnum type, String metaId, Map params);

    /**
     * 批量添加
     *
     * @param type
     * @param metaId
     * @param list
     */
    void addBatch(StorageEnum type, String metaId, List<Map> list);

    /**
     * 修改
     *
     * @param type
     * @param params
     */
    void edit(StorageEnum type, Map params);

    /**
     * 修改
     *
     * @param type
     * @param metaId
     * @param params
     */
    void edit(StorageEnum type, String metaId, Map params);

    /**
     * 批量修改
     *
     * @param type
     * @param metaId
     * @param list
     */
    void editBatch(StorageEnum type, String metaId, List<Map> list);

    /**
     * 删除
     *
     * @param type
     * @param id
     */
    void remove(StorageEnum type, String id);

    /**
     * 删除
     *
     * @param type
     * @param metaId
     * @param id
     */
    void remove(StorageEnum type, String metaId, String id);

    /**
     * 批量删除
     *
     * @param type
     * @param metaId
     * @param ids
     */
    void removeBatch(StorageEnum type, String metaId, List<String> ids);
}