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

    /**
     * 查询所有数据
     *
     * @param query
     * @return
     */
    Paging query(Query query);

    /**
     * 清空数据/日志
     *
     * @param type
     * @param collectionId
     */
    void clear(StorageEnum type, String collectionId);

    /**
     * 添加配置
     *
     * @param params
     */
    void addConfig(Map params);

    /**
     * 修改配置
     *
     * @param params
     */
    void editConfig(Map params);

    /**
     * 删除配置
     *
     * @param id
     */
    void removeConfig(String id);

    /**
     * 记录日志
     *
     * @param params
     */
    void addLog(Map<String,Object> params);

    /**
     * 记录数据
     *
     * @param collectionId
     * @param list
     */
    void addData(String collectionId, List<Map> list);
}