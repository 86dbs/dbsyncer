/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.GroupSearchFilter;
import org.dbsyncer.biz.GroupService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.GroupProfile;
import org.dbsyncer.parser.GroupRelProfile;
import org.dbsyncer.parser.model.Group;
import org.dbsyncer.parser.model.GroupRel;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link GroupService} 实现。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-14 18:00
 */
@Service
public class GroupServiceImpl implements GroupService {

    private static final int NAME_MAX_LEN = 50;

    private static final Set<String> RESOURCE_TYPES = new HashSet<>(Arrays.asList(
            ConfigConstant.CONNECTOR,
            ConfigConstant.MAPPING,
            ConfigConstant.VALIDATE_SYNC,
            ConfigConstant.DATABASE_SYNC
    ));

    @Resource
    private GroupProfile groupProfile;

    @Resource
    private GroupRelProfile groupRelProfile;

    @Override
    public String add(Map<String, String> params) {
        Assert.notNull(params, "参数不能为空");
        String name = validateAndNormalizeName(params.get(ConfigConstant.CONFIG_MODEL_NAME), null);
        long now = System.currentTimeMillis();
        Group group = new Group();
        group.setName(name);
        group.setCreateTime(now);
        group.setUpdateTime(now);
        return groupProfile.addGroup(group);
    }

    @Override
    public String edit(Map<String, String> params) {
        Assert.notNull(params, "参数不能为空");
        String id = StringUtil.trim(params.get(ConfigConstant.CONFIG_MODEL_ID));
        Assert.hasText(id, "分组ID不能为空");
        Group group = groupProfile.getGroup(id);
        Assert.notNull(group, "分组不存在");
        String name = validateAndNormalizeName(params.get(ConfigConstant.CONFIG_MODEL_NAME), id);
        group.setName(name);
        group.setUpdateTime(System.currentTimeMillis());
        return groupProfile.updateGroup(group);
    }

    @Override
    public String remove(String id) {
        Assert.hasText(id, "分组ID不能为空");
        Group group = groupProfile.getGroup(id);
        Assert.notNull(group, "分组不存在");
        // 铁律②：先清 rel 再删组
        groupRelProfile.removeByGroup(id);
        groupProfile.removeGroup(id);
        return id;
    }

    @Override
    public List<Group> list() {
        return groupProfile.listGroups();
    }

    @Override
    public Group get(String id) {
        return groupProfile.getGroup(id);
    }

    @Override
    public int countMembers(String id) {
        Assert.hasText(id, "分组ID不能为空");
        return groupRelProfile.countByGroup(id);
    }

    @Override
    public void bind(String groupId, String resourceType, String resourceId) {
        Assert.hasText(resourceType, "资源类型不能为空");
        Assert.hasText(resourceId, "资源ID不能为空");
        if (!RESOURCE_TYPES.contains(resourceType)) {
            throw new BizException("不支持的资源类型: " + resourceType);
        }
        // 空 groupId = 清除该资源全部归属
        if (StringUtil.isBlank(groupId)) {
            groupRelProfile.removeByResource(resourceType, resourceId);
            return;
        }
        Group group = groupProfile.getGroup(groupId);
        Assert.notNull(group, "分组不存在");
        if (groupRelProfile.getByGroupAndResource(groupId, resourceType, resourceId) != null) {
            return;
        }
        long now = System.currentTimeMillis();
        GroupRel rel = new GroupRel();
        rel.setGroupId(groupId);
        rel.setResourceType(resourceType);
        rel.setResourceId(resourceId);
        rel.setCreateTime(now);
        rel.setUpdateTime(now);
        groupRelProfile.add(rel);
    }

    @Override
    public void bindBatch(String groupId, String resourceType, List<String> resourceIds) {
        if (CollectionUtils.isEmpty(resourceIds)) {
            return;
        }
        for (String resourceId : resourceIds) {
            if (StringUtil.isBlank(resourceId)) {
                continue;
            }
            bind(groupId, resourceType, resourceId);
        }
    }

    @Override
    public void unbind(String groupId, String resourceType, String resourceId) {
        Assert.hasText(groupId, "分组ID不能为空");
        Assert.hasText(resourceType, "资源类型不能为空");
        Assert.hasText(resourceId, "资源ID不能为空");
        if (!RESOURCE_TYPES.contains(resourceType)) {
            throw new BizException("不支持的资源类型: " + resourceType);
        }
        groupRelProfile.removeByGroupAndResource(groupId, resourceType, resourceId);
    }

    @Override
    public void unbindBatch(String groupId, String resourceType, List<String> resourceIds) {
        if (CollectionUtils.isEmpty(resourceIds)) {
            return;
        }
        for (String resourceId : resourceIds) {
            if (StringUtil.isBlank(resourceId)) {
                continue;
            }
            unbind(groupId, resourceType, resourceId);
        }
    }

    @Override
    public List<GroupRel> listByResource(String resourceType, String resourceId) {
        return groupRelProfile.listByResource(resourceType, resourceId);
    }

    @Override
    public List<String> listResourceIds(String groupId, String resourceType) {
        return groupRelProfile.listResourceIds(groupId, resourceType);
    }

    @Override
    public GroupSearchFilter resolveSearchFilter(String groupIdParam, String resourceType) {
        // 空 / null = 不过滤（全部）
        if (StringUtil.isBlank(groupIdParam)) {
            return GroupSearchFilter.NONE;
        }
        List<String> ids = listResourceIds(groupIdParam, resourceType);
        if (CollectionUtils.isEmpty(ids)) {
            return GroupSearchFilter.EMPTY;
        }
        return new GroupSearchFilter(false, ids);
    }

    /**
     * 校验并规范化分组名称；excludeId 非空时允许同名属于自身（编辑场景）。
     */
    private String validateAndNormalizeName(String rawName, String excludeId) {
        String name = StringUtil.trim(rawName);
        Assert.hasText(name, "分组名称不能为空");
        if (name.length() > NAME_MAX_LEN) {
            throw new BizException("分组名称不能超过" + NAME_MAX_LEN + "个字符");
        }
        Group existing = groupProfile.getByName(name);
        if (existing != null && !StringUtil.equals(existing.getId(), excludeId)) {
            throw new BizException("分组名称已存在");
        }
        return name;
    }

}
