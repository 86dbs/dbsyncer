/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.NoticeMentionRuleService;
import org.dbsyncer.common.util.BooleanUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.GroupProfile;
import org.dbsyncer.parser.MappingProfile;
import org.dbsyncer.parser.NoticeMentionRuleProfile;
import org.dbsyncer.parser.model.Group;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.NoticeMentionRule;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * {@link NoticeMentionRuleService} 实现。
 *
 * @author wuji
 * @version 1.0.0
 */
@Service
public class NoticeMentionRuleServiceImpl implements NoticeMentionRuleService {

    @Resource
    private NoticeMentionRuleProfile noticeMentionRuleProfile;

    @Resource
    private GroupProfile groupProfile;

    @Resource
    private MappingProfile mappingProfile;

    @Override
    public List<NoticeMentionRule> listAll() {
        return noticeMentionRuleProfile.listAll();
    }

    @Override
    public synchronized String save(Map<String, String> params) {
        Assert.notNull(params, "参数不能为空");
        String scopeType = StringUtil.trim(params.get("scopeType"));
        String scopeId = StringUtil.trim(params.get("scopeId"));
        Assert.hasText(scopeType, "作用域类型不能为空");
        Assert.hasText(scopeId, "须选择分组或同步任务");
        Assert.isTrue(StringUtil.equals(ConfigConstant.GROUP, scopeType)
                        || StringUtil.equals(ConfigConstant.MAPPING, scopeType),
                "作用域仅支持分组或同步任务");
        assertScopeExists(scopeType, scopeId);

        boolean atAll = BooleanUtil.toBoolean(params.get("atAll"));
        String atMobiles = StringUtil.trim(params.get("atMobiles"));
        if (!atAll) {
            Assert.hasText(atMobiles, "未开启@所有人时须填写至少一个手机号");
        }

        String id = StringUtil.trim(params.get("id"));
        NoticeMentionRule conflict = noticeMentionRuleProfile.getByScope(scopeType, scopeId);
        if (StringUtil.isNotBlank(id)) {
            NoticeMentionRule existing = noticeMentionRuleProfile.get(id);
            Assert.notNull(existing, "提及规则不存在");
            Assert.isTrue(conflict == null || StringUtil.equals(conflict.getId(), id),
                    "同一分组/任务只能配置一条 @ 规则");
            existing.setScopeType(scopeType);
            existing.setScopeId(scopeId);
            existing.setAtAll(atAll ? 1 : 0);
            existing.setAtMobiles(atMobiles);
            existing.setUpdateTime(System.currentTimeMillis());
            return noticeMentionRuleProfile.update(existing);
        }

        Assert.isTrue(conflict == null, "同一分组/任务只能配置一条 @ 规则");
        long now = System.currentTimeMillis();
        NoticeMentionRule rule = new NoticeMentionRule();
        rule.setScopeType(scopeType);
        rule.setScopeId(scopeId);
        rule.setAtAll(atAll ? 1 : 0);
        rule.setAtMobiles(atMobiles);
        rule.setCreateTime(now);
        rule.setUpdateTime(now);
        return noticeMentionRuleProfile.add(rule);
    }

    @Override
    public synchronized String remove(Map<String, String> params) {
        Assert.notNull(params, "参数不能为空");
        String id = StringUtil.trim(params.get("id"));
        Assert.hasText(id, "规则ID不能为空");
        NoticeMentionRule existing = noticeMentionRuleProfile.get(id);
        Assert.notNull(existing, "提及规则不存在");
        noticeMentionRuleProfile.remove(id);
        return id;
    }

    private void assertScopeExists(String scopeType, String scopeId) {
        if (StringUtil.equals(ConfigConstant.GROUP, scopeType)) {
            Group group = groupProfile.getGroup(scopeId);
            Assert.notNull(group, "分组不存在");
            return;
        }
        Mapping mapping = mappingProfile.get(scopeId);
        Assert.notNull(mapping, "同步任务不存在");
        Assert.isTrue(StringUtil.equals(ConfigConstant.MAPPING, mapping.getType()), "同步任务不存在");
    }
}
