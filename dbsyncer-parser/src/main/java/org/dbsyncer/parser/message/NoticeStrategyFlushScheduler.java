/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.message;

import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.sdk.model.NoticeStrategy;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    private final Set<String> registeredIds = new HashSet<>();

    /**
     * 按策略列表重建周期任务。
     */
    public synchronized void reload(List<NoticeStrategy> strategies) {
        for (String id : registeredIds) {
            scheduledTaskService.stop(TASK_KEY_PREFIX + id);
        }
        registeredIds.clear();
        strategyBucketManager.clear();
        if (CollectionUtils.isEmpty(strategies)) {
            return;
        }
        for (NoticeStrategy strategy : strategies) {
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
        registeredIds.add(strategy.getId());
    }
}
