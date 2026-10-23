/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.message;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.NoticeStrategyConfig;
import org.dbsyncer.sdk.enums.NoticeFrequencyEnum;
import org.dbsyncer.sdk.enums.NoticeTypeEnum;
import org.dbsyncer.sdk.model.NoticeStrategy;
import org.dbsyncer.sdk.model.NoticeStrategyDef;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 告警策略：表模型 / DTO / 运行时互转。
 *
 * @author wuji
 * @version 1.0.0
 */
public final class NoticeStrategyConverter {

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

    public static List<NoticeStrategyDef> toDefs(List<NoticeStrategyConfig> configs) {
        List<NoticeStrategyDef> list = new ArrayList<>();
        if (CollectionUtils.isEmpty(configs)) {
            return list;
        }
        for (NoticeStrategyConfig config : configs) {
            NoticeStrategyDef def = toDef(config);
            list.add(def);
        }
        return list;
    }

    public static NoticeStrategyDef toDef(NoticeStrategyConfig config) {
        NoticeStrategyDef def = new NoticeStrategyDef();
        def.setId(config.getId());
        def.setName(config.getName());
        def.setEnabled(config.isEnabled());
        def.setFrequency(config.getFrequency());
        def.setCron(config.getCron());
        List<String> noticeTypes = config.getNoticeTypes() != null ? Arrays.stream(config.getNoticeTypes().split(StringUtil.COMMA))
                .map(String::trim)
                .filter(StringUtil::isNotBlank)
                .collect(Collectors.toList()) : new ArrayList<>();
        def.setNoticeTypes(noticeTypes);
        return def;
    }

    public static NoticeStrategyConfig toConfig(NoticeStrategyDef def) {
        if (def == null) {
            return null;
        }
        NoticeStrategyConfig config = new NoticeStrategyConfig();
        config.setId(def.getId());
        config.setName(StringUtil.getIfBlank(def.getName(), StringUtil.EMPTY));
        config.setEnabled(def.isEnabled() ? 1 : 0);
        config.setFrequency(def.getFrequency());
        config.setCron(def.getCron());
        String noticeTypes = def.getNoticeTypes().stream()
                .filter(StringUtil::isNotBlank)
                .map(String::trim)
                .collect(Collectors.joining(StringUtil.COMMA));
        config.setNoticeTypes(noticeTypes);
        long now = System.currentTimeMillis();
        config.setCreateTime(now);
        config.setUpdateTime(now);
        return config;
    }
}
