/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.message;

import org.dbsyncer.common.event.RemoveNoticeStrategyCacheEvent;
import org.dbsyncer.parser.NoticeStrategyProfile;
import org.dbsyncer.sdk.notice.NoticeDecisionEngine;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 策略表 → 决策引擎运行态的装载桥：独立于 {@link org.dbsyncer.sdk.notice.MessageService} 依赖链，
 * 避免 Engine 直接依赖 Profile 导致启动期循环依赖。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public class NoticeStrategyRuntimeBridge implements ApplicationListener<RemoveNoticeStrategyCacheEvent> {

    @Resource
    private NoticeStrategyProfile noticeStrategyProfile;

    @Resource
    private NoticeDecisionEngine noticeDecisionEngine;

    /**
     * 从策略表装载并刷新引擎运行态。
     */
    public void reloadFromStore() {
        noticeDecisionEngine.reload(
                NoticeStrategyConverter.toRuntime(NoticeStrategyConverter.toDefs(noticeStrategyProfile.listAll())));
    }

    @Override
    public void onApplicationEvent(RemoveNoticeStrategyCacheEvent event) {
        reloadFromStore();
    }
}
