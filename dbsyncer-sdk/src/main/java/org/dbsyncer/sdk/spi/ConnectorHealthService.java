/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

/**
 * 连接器健康检查服务：由开源绑定，供单机/集群控制面启停周期探测任务。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-10
 */
public interface ConnectorHealthService {

    /**
     * 启动连接器健康检查定时任务（幂等）。
     */
    void start();

    /**
     * 停止连接器健康检查定时任务（幂等）。
     */
    void stop();
}
