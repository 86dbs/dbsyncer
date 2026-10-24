/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.NoticeRuleProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.model.NoticeRule;
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
 * {@link NoticeRuleProfile} 实现。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public class NoticeRuleProfileImpl implements NoticeRuleProfile {

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private StorageService storageService;

    @Override
    public List<NoticeRule> listAll() {
        return operationTemplate.queryList(StorageEnum.NOTICE_RULE, new Query(), NoticeRule.class);
    }

    @Override
    public NoticeRule get(String id) {
        if (StringUtil.isBlank(id)) {
            return null;
        }
        return operationTemplate.queryObject(NoticeRule.class, id);
    }

    @Override
    public NoticeRule getByScope(String scopeType, String scopeId) {
        if (StringUtil.isBlank(scopeType) || StringUtil.isBlank(scopeId)) {
            return null;
        }
        Query query = new Query(1, 1);
        query.setType(StorageEnum.NOTICE_RULE);
        query.addFilter(ConfigConstant.NOTICE_RULE_SCOPE_TYPE, scopeType);
        query.addFilter(ConfigConstant.NOTICE_RULE_SCOPE_ID, scopeId);
        Paging paging = storageService.query(query);
        if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
            return null;
        }
        Object first = paging.getData().iterator().next();
        if (!(first instanceof Map)) {
            return null;
        }
        return ConfigModelUtil.parseFromRow((Map) first, NoticeRule.class);
    }

    @Override
    public List<NoticeRule> listByScopeType(String scopeType) {
        if (StringUtil.isBlank(scopeType)) {
            return Collections.emptyList();
        }
        Query condition = new Query();
        condition.addFilter(ConfigConstant.NOTICE_RULE_SCOPE_TYPE, scopeType);
        return operationTemplate.queryList(StorageEnum.NOTICE_RULE, condition, NoticeRule.class);
    }

    @Override
    public String add(NoticeRule rule) {
        return operationTemplate.execute(rule, CommandEnum.OPR_ADD);
    }

    @Override
    public String update(NoticeRule rule) {
        return operationTemplate.execute(rule, CommandEnum.OPR_EDIT);
    }

    @Override
    public void remove(String id) {
        if (StringUtil.isBlank(id)) {
            return;
        }
        storageService.remove(StorageEnum.NOTICE_RULE, id);
    }

    @Override
    public void removeByScopeId(String scopeType, String scopeId) {
        if (StringUtil.isBlank(scopeType) || StringUtil.isBlank(scopeId)) {
            return;
        }
        Query query = new Query();
        query.setType(StorageEnum.NOTICE_RULE);
        query.addFilter(ConfigConstant.NOTICE_RULE_SCOPE_TYPE, scopeType);
        query.addFilter(ConfigConstant.NOTICE_RULE_SCOPE_ID, scopeId);
        storageService.delete(query);
    }
}
