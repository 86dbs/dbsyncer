/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.GroupRelProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.model.GroupRel;
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
import java.util.stream.Collectors;

/**
 * {@link GroupRelProfile} 实现。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-14 18:00
 */
@Component
public class GroupRelProfileImpl implements GroupRelProfile {

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private StorageService storageService;

    @Override
    public GroupRel getByGroupAndResource(String groupId, String resourceType, String resourceId) {
        if (StringUtil.isBlank(groupId) || StringUtil.isBlank(resourceType) || StringUtil.isBlank(resourceId)) {
            return null;
        }
        Query query = new Query(1, 1);
        query.setType(StorageEnum.GROUP_REL);
        query.addFilter(ConfigConstant.GROUP_REL_GROUP_ID, groupId);
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
        return ConfigModelUtil.parseFromRow((Map) first, GroupRel.class);
    }

    @Override
    public List<GroupRel> listByResource(String resourceType, String resourceId) {
        if (StringUtil.isBlank(resourceType) || StringUtil.isBlank(resourceId)) {
            return Collections.emptyList();
        }
        Query condition = new Query();
        condition.addFilter(ConfigConstant.GROUP_REL_RESOURCE_TYPE, resourceType);
        condition.addFilter(ConfigConstant.GROUP_REL_RESOURCE_ID, resourceId);
        return operationTemplate.queryList(StorageEnum.GROUP_REL, condition, GroupRel.class);
    }

    @Override
    public List<GroupRel> listByGroup(String groupId, String resourceType) {
        if (StringUtil.isBlank(groupId)) {
            return Collections.emptyList();
        }
        Query condition = new Query();
        condition.addFilter(ConfigConstant.GROUP_REL_GROUP_ID, groupId);
        if (StringUtil.isNotBlank(resourceType)) {
            condition.addFilter(ConfigConstant.GROUP_REL_RESOURCE_TYPE, resourceType);
        }
        return operationTemplate.queryList(StorageEnum.GROUP_REL, condition, GroupRel.class);
    }

    @Override
    public List<String> listResourceIds(String groupId, String resourceType) {
        List<GroupRel> groupRel = listByGroup(groupId, resourceType);
        return CollectionUtils.isEmpty(groupRel) ? null : groupRel.stream().map(GroupRel::getResourceId).collect(Collectors.toList());
    }

    @Override
    public int countByGroup(String groupId) {
        if (StringUtil.isBlank(groupId)) {
            return 0;
        }
        Query condition = new Query();
        condition.addFilter(ConfigConstant.GROUP_REL_GROUP_ID, groupId);
        return operationTemplate.count(StorageEnum.GROUP_REL, condition);
    }

    @Override
    public String add(GroupRel rel) {
        return operationTemplate.execute(rel, CommandEnum.OPR_ADD);
    }

    @Override
    public String update(GroupRel rel) {
        return operationTemplate.execute(rel, CommandEnum.OPR_EDIT);
    }

    @Override
    public void remove(String id) {
        if (StringUtil.isBlank(id)) {
            return;
        }
        storageService.remove(StorageEnum.GROUP_REL, id);
    }

    @Override
    public void removeByGroupAndResource(String groupId, String resourceType, String resourceId) {
        GroupRel existing = getByGroupAndResource(groupId, resourceType, resourceId);
        if (existing != null) {
            remove(existing.getId());
        }
    }

    @Override
    public void removeByResource(String resourceType, String resourceId) {
        if (StringUtil.isBlank(resourceType) || StringUtil.isBlank(resourceId)) {
            return;
        }
        Query query = new Query();
        query.setType(StorageEnum.GROUP_REL);
        query.addFilter(ConfigConstant.GROUP_REL_RESOURCE_TYPE, resourceType);
        query.addFilter(ConfigConstant.GROUP_REL_RESOURCE_ID, resourceId);
        storageService.delete(query);
    }

    @Override
    public void removeByGroup(String groupId) {
        if (StringUtil.isBlank(groupId)) {
            return;
        }
        Query query = new Query();
        query.setType(StorageEnum.GROUP_REL);
        query.addFilter(ConfigConstant.GROUP_REL_GROUP_ID, groupId);
        storageService.delete(query);
    }
}
