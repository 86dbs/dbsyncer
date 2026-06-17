/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.message;

import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.sdk.model.NoticeStrategy;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * 周期策略 Cron 调度：到点 flush 缓冲队列并逐条发送。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-17 12:00
 */
@Component
public class NoticeStrategyFlushScheduler {

    private static final String TASK_KEY_PREFIX = "notice:strategy:";

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @Resource
    private StrategyBucketManager strategyBucketManager;

    @PostConstruct
    private void start() {
        for (NoticeStrategy strategy : DefaultNoticeStrategies.all()) {
            register(strategy);
        }
    }

    public void register(NoticeStrategy strategy) {
        if (strategy == null || !strategy.hasCron()) {
            return;
        }
        strategyBucketManager.register(strategy);
        String taskKey = TASK_KEY_PREFIX + strategy.getId();

        scheduledTaskService.start(taskKey, strategy.getCron(),
                () -> strategyBucketManager.flush(strategy.getId()));
    }
}
