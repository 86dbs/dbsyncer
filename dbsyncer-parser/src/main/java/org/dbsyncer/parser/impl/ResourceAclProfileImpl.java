/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ResourceAclProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.model.ResourceAcl;
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
 * {@link ResourceAclProfile} 实现。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
@Component
public class ResourceAclProfileImpl implements ResourceAclProfile {

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private StorageService storageService;

    @Override
    public ResourceAcl get(String id) {
        return operationTemplate.queryObject(ResourceAcl.class, id);
    }

    @Override
    public List<ResourceAcl> listAll() {
        return operationTemplate.queryList(StorageEnum.RESOURCE_ACL, new Query(), ResourceAcl.class);
    }

    @Override
    public List<ResourceAcl> listBySubject(String subjectType, String subjectId) {
        if (StringUtil.isBlank(subjectType) || StringUtil.isBlank(subjectId)) {
            return Collections.emptyList();
        }
        Query condition = new Query();
        condition.addFilter(ConfigConstant.RESOURCE_ACL_SUBJECT_TYPE, subjectType);
        condition.addFilter(ConfigConstant.RESOURCE_ACL_SUBJECT_ID, subjectId);
        return operationTemplate.queryList(StorageEnum.RESOURCE_ACL, condition, ResourceAcl.class);
    }

    @Override
    public ResourceAcl getByUnique(String subjectType, String subjectId, String resourceType, String resourceId) {
        if (StringUtil.isBlank(subjectType) || StringUtil.isBlank(subjectId)
                || StringUtil.isBlank(resourceType) || StringUtil.isBlank(resourceId)) {
            return null;
        }
        Query query = new Query(1, 1);
        query.setType(StorageEnum.RESOURCE_ACL);
        query.addFilter(ConfigConstant.RESOURCE_ACL_SUBJECT_TYPE, subjectType);
        query.addFilter(ConfigConstant.RESOURCE_ACL_SUBJECT_ID, subjectId);
        query.addFilter(ConfigConstant.GROUP_REL_RESOURCE_TYPE, resourceType);
        query.addFilter(ConfigConstant.GROUP_REL_RESOURCE_ID, resourceId);
        Paging paging = storageService.query(query);
        if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
            return null;
        }
        Object first = paging.getData().iterator().next();
        if (!(first instanceof Map)) {
            return null;
        }
        ResourceAcl model = ConfigModelUtil.parseFromRow((Map) first, ResourceAcl.class);
        if (model != null) {
            model.setType(ConfigConstant.RESOURCE_ACL);
        }
        return model;
    }

    @Override
    public String add(ResourceAcl acl) {
        return operationTemplate.execute(acl, CommandEnum.OPR_ADD);
    }

    @Override
    public String update(ResourceAcl acl) {
        return operationTemplate.execute(acl, CommandEnum.OPR_EDIT);
    }

    @Override
    public void remove(String id) {
        if (StringUtil.isBlank(id)) {
            return;
        }
        storageService.remove(StorageEnum.RESOURCE_ACL, id);
    }

    @Override
    public void removeByResource(String resourceType, String resourceId) {
        if (StringUtil.isBlank(resourceType) || StringUtil.isBlank(resourceId)) {
            return;
        }
        Query query = new Query();
        query.setType(StorageEnum.RESOURCE_ACL);
        query.addFilter(ConfigConstant.GROUP_REL_RESOURCE_TYPE, resourceType);
        query.addFilter(ConfigConstant.GROUP_REL_RESOURCE_ID, resourceId);
        storageService.delete(query);
    }

    @Override
    public void removeBySubject(String subjectType, String subjectId) {
        if (StringUtil.isBlank(subjectType) || StringUtil.isBlank(subjectId)) {
            return;
        }
        Query query = new Query();
        query.setType(StorageEnum.RESOURCE_ACL);
        query.addFilter(ConfigConstant.RESOURCE_ACL_SUBJECT_TYPE, subjectType);
        query.addFilter(ConfigConstant.RESOURCE_ACL_SUBJECT_ID, subjectId);
        storageService.delete(query);
    }
}
