/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.message;

import org.dbsyncer.sdk.model.NoticeContent;
import org.dbsyncer.sdk.model.NoticeStrategy;
import org.dbsyncer.sdk.notice.NoticeDecisionEngine;
import org.dbsyncer.sdk.notice.NoticeDeliveryHandler;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;

/**
 * 消息通知决策引擎：按策略匹配，立即发送或入队等待周期 flush。
 * <p>
 * 不依赖策略 Profile；装载由 {@link NoticeStrategyRuntimeBridge} / 业务 Service / 预加载完成。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-16 12:00
 */
@Component
public class NoticeDecisionEngineImpl implements NoticeDecisionEngine {

    private volatile NoticeStrategyRegistry strategyRegistry;

    @Resource
    private StrategyBucketManager strategyBucketManager;

    @Resource
    private NoticeDeliveryHandler noticeDeliveryHandler;

    @Resource
    private NoticeStrategyFlushScheduler noticeStrategyFlushScheduler;

    @Override
    public void dispatch(NoticeContent noticeContent) {
        if (noticeContent == null || noticeContent.getNoticeType() == null) {
            return;
        }
        NoticeStrategyRegistry registry = this.strategyRegistry;
        if (registry == null) {
            return;
        }
        List<NoticeStrategy> strategies = registry.match(noticeContent.getNoticeType());
        for (NoticeStrategy strategy : strategies) {
            if (strategy.isImmediate()) {
                noticeDeliveryHandler.deliver(noticeContent);
            } else {
                strategyBucketManager.offer(strategy, noticeContent);
            }
        }
    }

    @Override
    public synchronized void reload(List<NoticeStrategy> strategies) {
        List<NoticeStrategy> list = strategies == null ? Collections.emptyList() : strategies;
        this.strategyRegistry = new NoticeStrategyRegistry(list);
        noticeStrategyFlushScheduler.reload(list);
    }
}
