/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.message;

import org.dbsyncer.sdk.model.NoticeContent;
import org.dbsyncer.sdk.model.NoticeStrategy;
import org.dbsyncer.sdk.notice.NoticeDecisionEngine;
import org.dbsyncer.sdk.notice.NoticeDeliveryHandler;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;

/**
 * 消息通知决策引擎：按策略匹配，立即发送或入队等待周期 flush。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-16 12:00
 */
@Component
public class NoticeDecisionEngineImpl implements NoticeDecisionEngine {

    private NoticeStrategyRegistry strategyRegistry;

    @Resource
    private StrategyBucketManager strategyBucketManager;

    @Resource
    private NoticeDeliveryHandler noticeDeliveryHandler;

    @PostConstruct
    private void init() {
        List<NoticeStrategy> strategies = DefaultNoticeStrategy.all();
        this.strategyRegistry = new NoticeStrategyRegistry(strategies);
    }

    @Override
    public void dispatch(NoticeContent noticeContent) {
        if (noticeContent == null || noticeContent.getNoticeType() == null) {
            return;
        }
        List<NoticeStrategy> strategies = strategyRegistry.match(noticeContent.getNoticeType());
        for (NoticeStrategy strategy : strategies) {
            if (strategy.isImmediate()) {
                noticeDeliveryHandler.deliver(noticeContent);
            } else {
                strategyBucketManager.offer(strategy, noticeContent);
            }
        }
    }
}
