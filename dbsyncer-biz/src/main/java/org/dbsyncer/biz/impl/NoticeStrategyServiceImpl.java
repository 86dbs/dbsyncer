/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.NoticeStrategyService;
import org.dbsyncer.common.util.BooleanUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.NoticeStrategyProfile;
import org.dbsyncer.parser.message.NoticeStrategyConverter;
import org.dbsyncer.parser.message.NoticeStrategyRuntimeBridge;
import org.dbsyncer.parser.model.NoticeStrategyConfig;
import org.dbsyncer.sdk.enums.NoticeFrequencyEnum;
import org.dbsyncer.sdk.enums.NoticeTypeEnum;
import org.dbsyncer.sdk.model.NoticeStrategyDef;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

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
        assertNoticeTypeUnique(def.getNoticeTypes(), id);
        long now = System.currentTimeMillis();
        if (StringUtil.isNotBlank(id)) {
            NoticeStrategyConfig existing = noticeStrategyProfile.get(id);
            Assert.notNull(existing, "策略不存在");
            existing.setName(def.getName());
            existing.setEnabled(def.isEnabled() ? 1 : 0);
            existing.setFrequency(def.getFrequency());
            existing.setCron(def.getCron());
            existing.setNoticeTypes(def.getNoticeTypes());
            existing.setRemark(def.getRemark());
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
        int frequencyCode = parseFrequency(params.get("frequency"));
        NoticeFrequencyEnum frequency = NoticeFrequencyEnum.getByCode(frequencyCode);
        Assert.notNull(frequency, "策略频率无效");
        String cron = StringUtil.trim(params.get("cron"));
        if (frequency == NoticeFrequencyEnum.INTERVAL) {
            Assert.hasText(cron, "周期策略须填写 Cron");
        } else {
            cron = null;
        }
        int noticeTypes = parseNoticeType(params.get("noticeTypes"));
        Assert.notNull(NoticeTypeEnum.getByCode(noticeTypes), "通知类型无效");

        NoticeStrategyDef def = new NoticeStrategyDef();
        def.setName(name);
        def.setFrequency(frequencyCode);
        def.setCron(cron);
        def.setEnabled(params.containsKey("enabled") ? BooleanUtil.toBoolean(params.get("enabled")) : true);
        def.setNoticeTypes(noticeTypes);
        def.setRemark(StringUtil.trim(params.get("remark")));
        return def;
    }

    private void assertNoticeTypeUnique(int noticeTypes, String excludeId) {
        List<NoticeStrategyConfig> all = noticeStrategyProfile.listAll();
        if (all == null) {
            return;
        }
        for (NoticeStrategyConfig config : all) {
            if (config == null || config.getNoticeTypes() != noticeTypes) {
                continue;
            }
            if (StringUtil.isNotBlank(excludeId) && StringUtil.equals(excludeId, config.getId())) {
                continue;
            }
            throw new BizException("同一通知类型只能配置一条策略");
        }
    }

    private static int parseFrequency(String raw) {
        String value = StringUtil.trim(raw);
        Assert.hasText(value, "策略频率不能为空");
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ignore) {
            // 兼容枚举名
        }
        try {
            return NoticeFrequencyEnum.valueOf(value).getCode();
        } catch (Exception e) {
            throw new BizException("策略频率无效: " + value);
        }
    }

    private static int parseNoticeType(String raw) {
        String value = StringUtil.trim(raw);
        Assert.hasText(value, "策略须指定通知类型");
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ignore) {
            // 兼容枚举名
        }
        try {
            return NoticeTypeEnum.valueOf(value).getCode();
        } catch (Exception e) {
            throw new BizException("通知类型无效: " + value);
        }
    }
}
