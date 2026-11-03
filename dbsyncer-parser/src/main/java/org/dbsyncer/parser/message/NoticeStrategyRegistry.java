/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.message;

import org.dbsyncer.sdk.enums.NoticeTypeEnum;
import org.dbsyncer.sdk.model.NoticeStrategy;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * 事件类型与策略一对一注册表。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-16 12:00
 */
public final class NoticeStrategyRegistry {

    private final Map<NoticeTypeEnum, NoticeStrategy> strategyMap = new EnumMap<>(NoticeTypeEnum.class);

    public NoticeStrategyRegistry(List<NoticeStrategy> strategies) {
        if (strategies == null) {
            return;
        }
        for (NoticeStrategy strategy : strategies) {
            if (strategy == null || strategy.getNoticeType() == null) {
                continue;
            }
            strategyMap.putIfAbsent(strategy.getNoticeType(), strategy);
        }
    }

    /**
     * 按通知类型匹配唯一策略；未配置返回 {@code null}。
     */
    public NoticeStrategy match(NoticeTypeEnum noticeType) {
        if (noticeType == null) {
            return null;
        }
        return strategyMap.get(noticeType);
    }
}
