/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.message;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.enums.NoticeFrequencyEnum;
import org.dbsyncer.sdk.enums.NoticeTypeEnum;
import org.dbsyncer.sdk.model.NoticeConfig;
import org.dbsyncer.sdk.model.NoticeStrategy;
import org.dbsyncer.sdk.model.NoticeStrategyDef;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * 告警策略定义与运行时策略互转；空配置时种子内置默认策略。
 *
 * @author wuji
 * @version 1.0.0
 */
public final class NoticeStrategyConverter {

    private NoticeStrategyConverter() {
    }

    /**
     * 确保配置中有策略列表；为空则写入内置默认并返回运行时策略（仅启用项）。
     */
    public static List<NoticeStrategy> ensureAndResolve(NoticeConfig noticeConfig) {
        if (noticeConfig == null) {
            return toRuntime(DefaultNoticeStrategy.allDefs());
        }
        if (CollectionUtils.isEmpty(noticeConfig.getStrategies())) {
            noticeConfig.setStrategies(DefaultNoticeStrategy.allDefs());
        }
        return toRuntime(noticeConfig.getStrategies());
    }

    public static List<NoticeStrategy> toRuntime(List<NoticeStrategyDef> defs) {
        List<NoticeStrategy> list = new ArrayList<>();
        if (CollectionUtils.isEmpty(defs)) {
            return list;
        }
        for (NoticeStrategyDef def : defs) {
            NoticeStrategy strategy = toRuntime(def);
            if (strategy != null) {
                list.add(strategy);
            }
        }
        return list;
    }

    public static NoticeStrategy toRuntime(NoticeStrategyDef def) {
        if (def == null || !def.isEnabled() || StringUtil.isBlank(def.getId())) {
            return null;
        }
        NoticeFrequencyEnum frequency;
        try {
            frequency = NoticeFrequencyEnum.valueOf(def.getFrequency());
        } catch (Exception e) {
            return null;
        }
        Set<NoticeTypeEnum> types = EnumSet.noneOf(NoticeTypeEnum.class);
        if (!CollectionUtils.isEmpty(def.getNoticeTypes())) {
            for (String name : def.getNoticeTypes()) {
                if (StringUtil.isBlank(name)) {
                    continue;
                }
                try {
                    types.add(NoticeTypeEnum.valueOf(name.trim()));
                } catch (IllegalArgumentException ignore) {
                    // skip unknown
                }
            }
        }
        if (types.isEmpty()) {
            return null;
        }
        String cron = frequency == NoticeFrequencyEnum.INTERVAL ? def.getCron() : null;
        return new NoticeStrategy(def.getId(), def.getName(), frequency, cron, types);
    }

    public static NoticeStrategyDef toDef(NoticeStrategy strategy) {
        NoticeStrategyDef def = new NoticeStrategyDef();
        def.setId(strategy.getId());
        def.setName(strategy.getName());
        def.setEnabled(true);
        def.setFrequency(strategy.getFrequency().name());
        def.setCron(strategy.getCron());
        List<String> types = new ArrayList<>();
        for (NoticeTypeEnum type : strategy.getNoticeTypes()) {
            types.add(type.name());
        }
        def.setNoticeTypes(types);
        return def;
    }
}
