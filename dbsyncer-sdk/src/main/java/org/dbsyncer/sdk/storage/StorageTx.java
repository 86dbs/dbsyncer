/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage;

import java.util.List;
import java.util.Map;

/**
 * 同一 JDBC 连接上的查询/更新（用于 {@link StorageService#executeInTransaction}）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-04
 */
public interface StorageTx {

    /**
     * 执行查询。
     *
     * @param query SQL
     * @return 行列表
     */
    List<Map<String, Object>> queryList(SqlQuery query);

    /**
     * 执行更新。
     *
     * @param query SQL
     * @return 影响行数
     */
    int executeUpdate(SqlQuery query);
}
