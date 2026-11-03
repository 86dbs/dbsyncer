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
import java.util.List;

/**
 * 告警策略：表模型 / DTO / 运行时互转。
 *
 * @author wuji
 * @version 1.0.0
 */
public final class NoticeStrategyConverter {

    private NoticeStrategyConverter() {
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
        NoticeFrequencyEnum frequency = NoticeFrequencyEnum.getByCode(def.getFrequency());
        NoticeTypeEnum noticeType = NoticeTypeEnum.getByCode(def.getNoticeTypes());
        if (frequency == null || noticeType == null) {
            return null;
        }
        String cron = frequency == NoticeFrequencyEnum.INTERVAL ? def.getCron() : null;
        return new NoticeStrategy(def.getId(), def.getName(), frequency, cron, noticeType);
    }

    public static List<NoticeStrategyDef> toDefs(List<NoticeStrategyConfig> configs) {
        List<NoticeStrategyDef> list = new ArrayList<>();
        if (CollectionUtils.isEmpty(configs)) {
            return list;
        }
        for (NoticeStrategyConfig config : configs) {
            list.add(toDef(config));
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
        def.setNoticeTypes(config.getNoticeTypes());
        def.setRemark(config.getRemark());
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
        config.setNoticeTypes(def.getNoticeTypes());
        config.setRemark(StringUtil.getIfBlank(def.getRemark(), StringUtil.EMPTY));
        long now = System.currentTimeMillis();
        config.setCreateTime(now);
        config.setUpdateTime(now);
        return config;
    }
}
