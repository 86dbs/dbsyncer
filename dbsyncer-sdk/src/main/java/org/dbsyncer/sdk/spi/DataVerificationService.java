/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

/**
 * 数据校验服务
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-05-12 23:36
 */
public interface DataVerificationService {

    /**
     * 数据校验
     *
     * @param mappingId 驱动id
     * @return 任务id
     */
    String verify(String taskId);

}