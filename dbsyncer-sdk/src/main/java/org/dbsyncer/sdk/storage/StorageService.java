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
     * 确保分片表存在（仅建表结构，不写数据）。
     * <p>用于配置导入预建 {@link StorageEnum#TASK_DETAIL} 等动态分表。
     *
     * @param type   存储类型
     * @param metaId 分片键（TASK_DETAIL 为任务 ID）
     */
    void ensure(StorageEnum type, String metaId);

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

    /**
     * 原子增量更新（按列自增，如 COL = GREATEST(COL + ?, 0)），用于严格走库下的 Meta 计数；结果小于 0 时钳为 0
     *
     * @param type   存储类型
     * @param id     记录主键
     * @param deltas 列增量（key 为列 labelName，value 为增量值）
     */
    void increment(StorageEnum type, String id, Map<String, Long> deltas);

    /**
     * 条件更新：仅当 {@code casField=casValue} 时写入 {@code params} 中的列，避免整行覆盖。
     *
     * @param type     存储类型
     * @param id       记录主键
     * @param params   待更新列（key 为 labelName）
     * @param casField 条件列 labelName
     * @param casValue 条件列期望值
     * @return 影响行数，0 表示条件不匹配
     */
    int compareAndEdit(StorageEnum type, String id, Map params, String casField, Object casValue);

    /**
     * 条件更新，并可在同一条 SQL 内原子累加计数（进度与 success 同事务，避免改派漏计/双计）。
     *
     * @param type        存储类型
     * @param id          记录主键
     * @param params      待更新列
     * @param increments  列增量（可为 null）
     * @param casField    条件列
     * @param casValue    条件列期望值
     * @return 影响行数
     */
    int compareAndEdit(StorageEnum type, String id, Map params, Map<String, Long> increments, String casField, Object casValue);

    /**
     * 执行原生查询 SQL（系统配置库）；{@link SqlQuery#isPaged()} 为 true 时带分页。
     *
     * @param query SQL 与可选分页参数
     * @return 行列表，无结果时空列表
     */
    List<Map<String, Object>> queryList(SqlQuery query);
}
