/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.MappingProfile;
import org.dbsyncer.parser.MetaAggProfile;
import org.dbsyncer.parser.TaskMetaProfile;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.MetaAgg;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.service.ScheduledScanService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 同步任务 Meta 日汇总：由 {@link ScheduledScanService} 启停，集群仅 Leader 执行。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public class MetaAggCollector implements ScheduledScanService, ScheduledTaskJob, DisposableBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String SCHEDULE_KEY = "meta.daily.collector";

    /**
     * 30 分钟
     */
    private static final long PERIOD_MS = 30L * 60L * 1000L;

    /**
     * 保留天数（含今天）
     */
    private static final int RETAIN_DAYS = 30;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @Resource
    private TaskMetaProfile taskMetaProfile;

    @Resource
    private MappingProfile mappingProfile;

    @Resource
    private MetaAggProfile metaAggProfile;

    private final AtomicBoolean collecting = new AtomicBoolean(false);

    private volatile boolean started;

    @Override
    public synchronized void start() {
        if (started) {
            return;
        }
        scheduledTaskService.start(SCHEDULE_KEY, PERIOD_MS, this);
        started = true;
        // 启动立即跑一轮，避免首页历史为空
        run();
    }

    @Override
    public synchronized void stop() {
        if (!started) {
            return;
        }
        scheduledTaskService.stop(SCHEDULE_KEY);
        started = false;
    }

    @Override
    public void destroy() {
        stop();
    }

    @Override
    public void run() {
        if (!collecting.compareAndSet(false, true)) {
            return;
        }
        try {
            collectAndCleanup();
        } catch (Exception e) {
            logger.error("meta daily collect failed: {}", e.getMessage(), e);
        } finally {
            collecting.set(false);
        }
    }

    private void collectAndCleanup() {
        AtomicLong total = new AtomicLong();
        AtomicLong success = new AtomicLong();
        AtomicLong fail = new AtomicLong();
        taskMetaProfile.pageScanMetas(TaskLevelEnum.TASK.getCode(), ConfigConstant.PAGE_SIZE, page -> sumMappingMeta(page, total, success, fail));

        String today = LocalDate.now().format(DateFormatUtil.YYYY_MM_DD);
        MetaAgg metaAgg = new MetaAgg();
        metaAgg.setStatDate(today);
        metaAgg.setTotal(total.get());
        metaAgg.setSuccess(success.get());
        metaAgg.setFail(fail.get());
        metaAggProfile.upsert(metaAgg);

        String cutoff = LocalDate.now().minusDays(RETAIN_DAYS).format(DateFormatUtil.YYYY_MM_DD);
        metaAggProfile.removeBefore(cutoff);
    }

    private void sumMappingMeta(List<Meta> page, AtomicLong total, AtomicLong success, AtomicLong fail) {
        if (CollectionUtils.isEmpty(page)) {
            return;
        }
        for (Meta meta : page) {
            if (meta == null || StringUtil.isBlank(meta.getTaskId())) {
                continue;
            }
            if (mappingProfile.get(meta.getTaskId()) == null) {
                continue;
            }
            if (meta.getTotal() != null) {
                total.addAndGet(meta.getTotal().get());
            }
            if (meta.getSuccess() != null) {
                success.addAndGet(meta.getSuccess().get());
            }
            if (meta.getFail() != null) {
                fail.addAndGet(meta.getFail().get());
            }
        }
    }
}
