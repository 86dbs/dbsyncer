/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 存储服务（支持记录配置/日志/同步数据）
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2019-11-16 23:22
 */
public interface StorageService {

    /**
     * 初始化
     */
    void init(Properties properties);

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
    void add(StorageEnum type, Map params) throws Exception;

    /**
     * 添加
     *
     * @param type
     * @param metaId
     * @param params
     */
    void add(StorageEnum type, String metaId, Map params) throws Exception;

    /**
     * 批量添加
     *
     * @param type
     * @param metaId
     * @param list
     */
    void addBatch(StorageEnum type, String metaId, List<Map> list) throws Exception;

    /**
     * 修改
     *
     * @param type
     * @param params
     */
    void edit(StorageEnum type, Map params) throws Exception;

    /**
     * 修改
     *
     * @param type
     * @param metaId
     * @param params
     */
    void edit(StorageEnum type, String metaId, Map params) throws Exception;

    /**
     * 批量修改
     *
     * @param type
     * @param metaId
     * @param list
     */
    void editBatch(StorageEnum type, String metaId, List<Map> list) throws Exception;

    /**
     * 删除
     *
     * @param type
     * @param id
     */
    void remove(StorageEnum type, String id) throws Exception;

    /**
     * 删除
     *
     * @param type
     * @param metaId
     * @param id
     */
    void remove(StorageEnum type, String metaId, String id) throws Exception;

    /**
     * 批量删除
     *
     * @param type
     * @param metaId
     * @param ids
     */
    void removeBatch(StorageEnum type, String metaId, List<String> ids) throws Exception;
}