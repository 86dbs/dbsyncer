/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.message;

import org.dbsyncer.sdk.enums.NoticeTypeEnum;
import org.dbsyncer.sdk.model.NoticeStrategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * 事件类型与策略的映射注册表。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-16 12:00
 */
public final class NoticeStrategyRegistry {

    private final Map<NoticeTypeEnum, List<NoticeStrategy>> strategyMap = new EnumMap<>(NoticeTypeEnum.class);

    public NoticeStrategyRegistry(List<NoticeStrategy> strategies) {
        for (NoticeStrategy strategy : strategies) {
            for (NoticeTypeEnum noticeType : strategy.getNoticeTypes()) {
                strategyMap.computeIfAbsent(noticeType, key -> new ArrayList<>()).add(strategy);
            }
        }
        strategyMap.replaceAll((type, list) -> Collections.unmodifiableList(list));
    }

    public List<NoticeStrategy> match(NoticeTypeEnum noticeType) {
        if (noticeType == null) {
            return Collections.emptyList();
        }
        return strategyMap.getOrDefault(noticeType, Collections.emptyList());
    }
}
