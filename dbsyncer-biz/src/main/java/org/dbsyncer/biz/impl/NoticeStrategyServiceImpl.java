/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.NoticeStrategyService;
import org.dbsyncer.common.util.BooleanUtil;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.NoticeStrategyProfile;
import org.dbsyncer.parser.message.NoticeStrategyConverter;
import org.dbsyncer.parser.message.NoticeStrategyRuntimeBridge;
import org.dbsyncer.parser.model.NoticeStrategyConfig;
import org.dbsyncer.sdk.enums.NoticeFrequencyEnum;
import org.dbsyncer.sdk.model.NoticeStrategyDef;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link NoticeStrategyService} 实现。
 *
 * @author wuji
 * @version 1.0.0
 */
@Service
public class NoticeStrategyServiceImpl implements NoticeStrategyService {

    @Resource
    private NoticeStrategyProfile noticeStrategyProfile;

    @Resource
    private NoticeStrategyRuntimeBridge noticeStrategyRuntimeBridge;

    @Override
    public List<NoticeStrategyDef> listAll() {
        return NoticeStrategyConverter.toDefs(noticeStrategyProfile.listAll());
    }

    @Override
    public synchronized String save(Map<String, String> params) {
        Assert.notNull(params, "参数不能为空");
        NoticeStrategyDef def = parseOne(params);
        String id = StringUtil.trim(params.get("id"));
        long now = System.currentTimeMillis();
        if (StringUtil.isNotBlank(id)) {
            NoticeStrategyConfig existing = noticeStrategyProfile.get(id);
            Assert.notNull(existing, "策略不存在");
            existing.setName(def.getName());
            existing.setEnabled(def.isEnabled() ? 1 : 0);
            existing.setFrequency(def.getFrequency());
            existing.setCron(def.getCron());
            existing.setNoticeTypes(joinTypes(def.getNoticeTypes()));
            existing.setUpdateTime(now);
            String result = noticeStrategyProfile.update(existing);
            noticeStrategyRuntimeBridge.reloadFromStore();
            return result;
        }

        NoticeStrategyConfig config = NoticeStrategyConverter.toConfig(def);
        Assert.notNull(config, "策略无效");
        // 新增由存储层生成 ID
        config.setId(null);
        config.setCreateTime(now);
        config.setUpdateTime(now);
        String result = noticeStrategyProfile.add(config);
        noticeStrategyRuntimeBridge.reloadFromStore();
        return result;
    }

    @Override
    public synchronized String remove(Map<String, String> params) {
        Assert.notNull(params, "参数不能为空");
        String id = StringUtil.trim(params.get("id"));
        Assert.hasText(id, "策略ID不能为空");
        NoticeStrategyConfig existing = noticeStrategyProfile.get(id);
        Assert.notNull(existing, "策略不存在");
        noticeStrategyProfile.remove(id);
        noticeStrategyRuntimeBridge.reloadFromStore();
        return id;
    }

    private NoticeStrategyDef parseOne(Map<String, String> params) {
        String name = StringUtil.trim(params.get("name"));
        Assert.hasText(name, "策略名称不能为空");
        String frequencyRaw = StringUtil.trim(params.get("frequency"));
        Assert.hasText(frequencyRaw, "策略频率不能为空");
        NoticeFrequencyEnum frequency;
        try {
            frequency = NoticeFrequencyEnum.valueOf(frequencyRaw);
        } catch (Exception e) {
            throw new BizException("策略频率无效: " + frequencyRaw);
        }
        String cron = StringUtil.trim(params.get("cron"));
        if (frequency == NoticeFrequencyEnum.INTERVAL) {
            Assert.hasText(cron, "周期策略须填写 Cron");
        } else {
            cron = null;
        }
        List<String> noticeTypes = splitTypes(params.get("noticeTypes"));
        Assert.isTrue(!CollectionUtils.isEmpty(noticeTypes), "策略须至少选择一种通知类型");

        NoticeStrategyDef def = new NoticeStrategyDef();
        def.setName(name);
        def.setFrequency(frequency.name());
        def.setCron(cron);
        def.setEnabled(params.containsKey("enabled") ? BooleanUtil.toBoolean(params.get("enabled")) : true);
        def.setNoticeTypes(noticeTypes);
        return def;
    }

    private List<String> splitTypes(String noticeTypes) {
        if (StringUtil.isBlank(noticeTypes)) {
            return new ArrayList<>();
        }
        return Arrays.stream(noticeTypes.split(StringUtil.COMMA))
                .map(String::trim)
                .filter(StringUtil::isNotBlank)
                .collect(Collectors.toList());
    }

    private String joinTypes(List<String> noticeTypes) {
        if (CollectionUtils.isEmpty(noticeTypes)) {
            return StringUtil.EMPTY;
        }
        return noticeTypes.stream()
                .filter(StringUtil::isNotBlank)
                .map(String::trim)
                .collect(Collectors.joining(StringUtil.COMMA));
    }
}
