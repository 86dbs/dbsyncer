/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.message;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.enums.NoticeTypeEnum;
import org.dbsyncer.sdk.model.NoticeContent;
import org.dbsyncer.sdk.model.NoticeStrategy;
import org.dbsyncer.sdk.notice.NoticeDeliveryHandler;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 周期策略待发送队列：同策略桶内按消息键去重，flush 时逐条投递。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-17 12:00
 */
@Component
public class StrategyBucketManager {

    private final Map<String, Map<String, NoticeContent>> buckets = new ConcurrentHashMap<>();

    @Resource
    private NoticeDeliveryHandler noticeDeliveryHandler;

    public void register(NoticeStrategy strategy) {
        if (strategy != null && strategy.isInterval()) {
            buckets.computeIfAbsent(strategy.getId(),
                    key -> Collections.synchronizedMap(new LinkedHashMap<>()));
        }
    }

    public void offer(NoticeStrategy strategy, NoticeContent content) {
        if (strategy == null || content == null || !strategy.isInterval()) {
            return;
        }
        String dedupeKey = resolve(content);
        if (StringUtil.isBlank(dedupeKey)) {
            return;
        }
        Map<String, NoticeContent> buffer = buckets.get(strategy.getId());
        if (buffer == null) {
            return;
        }
        synchronized (buffer) {
            buffer.putIfAbsent(dedupeKey, content);
        }
    }

    public void flush(String strategyId) {
        List<NoticeContent> snapshot = drain(strategyId);
        for (NoticeContent content : snapshot) {
            noticeDeliveryHandler.deliver(content);
        }
    }

    private List<NoticeContent> drain(String strategyId) {
        Map<String, NoticeContent> buffer = buckets.get(strategyId);
        if (buffer == null) {
            return Collections.emptyList();
        }
        synchronized (buffer) {
            if (buffer.isEmpty()) {
                return Collections.emptyList();
            }
            List<NoticeContent> snapshot = new ArrayList<>(buffer.values());
            buffer.clear();
            return snapshot;
        }
    }

    public static String resolve(NoticeContent content) {
        if (content == null) {
            return StringUtil.EMPTY;
        }
        NoticeTypeEnum noticeType = content.getNoticeType();
        String type = noticeType == null ? StringUtil.EMPTY : noticeType.name();
        String title = content.getTitle() == null ? StringUtil.EMPTY : content.getTitle();
        return type + '：' + title;
    }
}
