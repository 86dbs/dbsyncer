/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.message;

import org.dbsyncer.parser.SystemConfigProfile;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.sdk.model.NoticeConfig;
import org.dbsyncer.sdk.model.NoticeContent;
import org.dbsyncer.sdk.model.NoticeStrategy;
import org.dbsyncer.sdk.notice.NoticeDecisionEngine;
import org.dbsyncer.sdk.notice.NoticeDeliveryHandler;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;

/**
 * 消息通知决策引擎：按策略匹配，立即发送或入队等待周期 flush。
 * <p>
 * 启动阶段仅装载内置默认策略，避免与 {@link SystemConfigProfile} 形成循环依赖；
 * 应用就绪后再从配置 reload。
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

    @Resource
    private SystemConfigProfile systemConfigProfile;

    @PostConstruct
    private void init() {
        applyStrategies(NoticeStrategyConverter.toRuntime(DefaultNoticeStrategy.allDefs()));
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        reload();
    }

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
    public synchronized void reload() {
        NoticeConfig noticeConfig = resolveNoticeConfig();
        applyStrategies(NoticeStrategyConverter.ensureAndResolve(noticeConfig));
    }

    private void applyStrategies(List<NoticeStrategy> strategies) {
        this.strategyRegistry = new NoticeStrategyRegistry(strategies);
        noticeStrategyFlushScheduler.reload(strategies);
    }

    private NoticeConfig resolveNoticeConfig() {
        if (systemConfigProfile == null) {
            return null;
        }
        SystemConfig systemConfig = systemConfigProfile.getSystemConfig();
        if (systemConfig == null) {
            return null;
        }
        if (systemConfig.getNoticeConfig() == null) {
            systemConfig.setNoticeConfig(new NoticeConfig());
        }
        return systemConfig.getNoticeConfig();
    }
}
