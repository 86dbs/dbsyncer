/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage;

/**
 * 配置库事务回调。
 *
 * @param <T> 返回类型
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-04
 */
@FunctionalInterface
public interface StorageTransactionCallback<T> {

    /**
     * 在同一连接事务中执行。
     *
     * @param tx 事务内操作
     * @return 业务结果
     * @throws Exception 任意异常将触发回滚
     */
    T doInTransaction(StorageTx tx) throws Exception;
}
