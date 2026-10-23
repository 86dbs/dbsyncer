/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.NoticeMentionRuleProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.model.NoticeMentionRule;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.storage.StorageService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * {@link NoticeMentionRuleProfile} 实现。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public class NoticeMentionRuleProfileImpl implements NoticeMentionRuleProfile {

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private StorageService storageService;

    @Override
    public List<NoticeMentionRule> listAll() {
        return operationTemplate.queryList(StorageEnum.NOTICE_MENTION_RULE, new Query(), NoticeMentionRule.class);
    }

    @Override
    public NoticeMentionRule get(String id) {
        if (StringUtil.isBlank(id)) {
            return null;
        }
        return operationTemplate.queryObject(NoticeMentionRule.class, id);
    }

    @Override
    public NoticeMentionRule getByScope(String scopeType, String scopeId) {
        if (StringUtil.isBlank(scopeType) || StringUtil.isBlank(scopeId)) {
            return null;
        }
        Query query = new Query(1, 1);
        query.setType(StorageEnum.NOTICE_MENTION_RULE);
        query.addFilter(ConfigConstant.NOTICE_MENTION_SCOPE_TYPE, scopeType);
        query.addFilter(ConfigConstant.NOTICE_MENTION_SCOPE_ID, scopeId);
        Paging paging = storageService.query(query);
        if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
            return null;
        }
        Object first = paging.getData().iterator().next();
        if (!(first instanceof Map)) {
            return null;
        }
        return ConfigModelUtil.parseFromRow((Map) first, NoticeMentionRule.class);
    }

    @Override
    public List<NoticeMentionRule> listByScopeType(String scopeType) {
        if (StringUtil.isBlank(scopeType)) {
            return Collections.emptyList();
        }
        Query condition = new Query();
        condition.addFilter(ConfigConstant.NOTICE_MENTION_SCOPE_TYPE, scopeType);
        return operationTemplate.queryList(StorageEnum.NOTICE_MENTION_RULE, condition, NoticeMentionRule.class);
    }

    @Override
    public String add(NoticeMentionRule rule) {
        return operationTemplate.execute(rule, CommandEnum.OPR_ADD);
    }

    @Override
    public String update(NoticeMentionRule rule) {
        return operationTemplate.execute(rule, CommandEnum.OPR_EDIT);
    }

    @Override
    public void remove(String id) {
        if (StringUtil.isBlank(id)) {
            return;
        }
        storageService.remove(StorageEnum.NOTICE_MENTION_RULE, id);
    }
}
