/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.sdk.spi.ConnectorHealthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * 连接器健康检查服务
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-10
 */
@Service
public final class ConnectorHealthServiceImpl implements ConnectorHealthService, DisposableBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String CONNECTOR_HEALTH_KEY = "connector-health";
    private static final long PERIOD_MS = 10_000L;

    @Resource
    private ConnectorService connectorService;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    private volatile boolean started;

    @Override
    public synchronized void start() {
        if (started) {
            return;
        }
        scheduledTaskService.start(CONNECTOR_HEALTH_KEY, PERIOD_MS, connectorService::refreshHealth);
        started = true;
        logger.info("connector health scheduler started");
    }

    @Override
    public synchronized void stop() {
        if (!started) {
            return;
        }
        scheduledTaskService.stop(CONNECTOR_HEALTH_KEY);
        started = false;
        logger.info("connector health scheduler stopped");
    }

    @Override
    public void destroy() {
        stop();
    }
}
