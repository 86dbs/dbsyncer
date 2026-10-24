/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.GroupSearchFilter;
import org.dbsyncer.biz.PermissionChecker;
import org.dbsyncer.biz.PermissionService;
import org.dbsyncer.biz.ResourceAclService;
import org.dbsyncer.biz.UserConfigService;
import org.dbsyncer.biz.enums.ResourceAclPermissionEnum;
import org.dbsyncer.biz.enums.ResourceAclSubjectEnum;
import org.dbsyncer.biz.enums.ResourceTypeEnum;
import org.dbsyncer.biz.vo.ResourceAclVO;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ConnectorProfile;
import org.dbsyncer.parser.DatabaseSyncProfile;
import org.dbsyncer.parser.GroupProfile;
import org.dbsyncer.parser.GroupRelProfile;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.MappingProfile;
import org.dbsyncer.parser.RbacProfile;
import org.dbsyncer.parser.ResourceAclProfile;
import org.dbsyncer.parser.UserProfile;
import org.dbsyncer.parser.ValidateSyncProfile;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Group;
import org.dbsyncer.parser.model.GroupRel;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.ResourceAcl;
import org.dbsyncer.parser.model.Role;
import org.dbsyncer.parser.model.UserInfo;
import org.dbsyncer.parser.model.UserRole;
import org.dbsyncer.sdk.model.DatabaseSyncTask;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link ResourceAclService} 实现。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
@Service
public class ResourceAclServiceImpl implements ResourceAclService {

    private static final String PERM_ACL_EDIT = "acl:edit";

    @Resource
    private ResourceAclProfile resourceAclProfile;

    @Resource
    private GroupProfile groupProfile;

    @Resource
    private GroupRelProfile groupRelProfile;

    @Resource
    private ConnectorProfile connectorProfile;

    @Resource
    private MappingProfile mappingProfile;

    @Resource
    private ValidateSyncProfile validateSyncProfile;

    @Resource
    private DatabaseSyncProfile databaseSyncProfile;

    @Resource
    private RbacProfile rbacProfile;

    @Resource
    private UserProfile userProfile;

    @Resource
    private PermissionService permissionService;

    @Resource
    private PermissionChecker permissionChecker;

    @Resource
    private LogService logService;

    @Override
    public List<ResourceAclVO> listAll() {
        List<ResourceAcl> list = resourceAclProfile.listAll();
        if (CollectionUtils.isEmpty(list)) {
            return Collections.emptyList();
        }
        List<ResourceAclVO> result = new ArrayList<>(list.size());
        for (ResourceAcl acl : list) {
            if (acl != null) {
                result.add(toResourceAclVO(acl));
            }
        }
        result.sort(Comparator
                .comparingInt(ResourceAclVO::getSubjectType)
                .thenComparing(ResourceAclVO::getSubjectName, Comparator.nullsLast(String::compareTo))
                .thenComparing(ResourceAclVO::getResourceName, Comparator.nullsLast(String::compareTo)));
        return result;
    }

    @Override
    public ResourceAclVO get(String id) {
        Assert.hasText(id, "授权ID不能为空");
        ResourceAcl acl = resourceAclProfile.get(id);
        Assert.notNull(acl, "授权不存在");
        return toResourceAclVO(acl);
    }

    @Override
    public synchronized String save(Map<String, String> params) {
        Assert.notNull(params, "参数不能为空");
        String currentUserName = params.get(UserConfigService.CURRENT_USER_NAME);
        permissionChecker.requirePermission(currentUserName, PERM_ACL_EDIT);

        int subjectType = normalizeSubjectType(params.get("subjectType"));
        String subjectId = resolveSubjectId(subjectType, params.get("subjectId"), params.get("username"));
        int permission = normalizePermission(params.get("permission"));

        String id = params.get("id");
        // 编辑单条：带 id 时只更新该记录
        if (StringUtil.isNotBlank(id)) {
            String resourceType = normalizeResourceType(params.get("resourceType"));
            List<String> resourceIds = parseResourceIds(params.get("resourceIds"), params.get("resourceId"));
            Assert.isTrue(!CollectionUtils.isEmpty(resourceIds), "请选择资源");
            String resourceId = resourceIds.get(0);
            assertResourceExists(resourceType, resourceId);
            ResourceAcl existing = resourceAclProfile.get(id);
            Assert.notNull(existing, "授权不存在");
            ResourceAcl conflict = resourceAclProfile.getByUnique(subjectType, subjectId, resourceType, resourceId);
            Assert.isTrue(conflict == null || StringUtil.equals(conflict.getId(), id),
                    "该主体已存在相同资源授权，请勿重复");
            existing.setSubjectType(subjectType);
            existing.setSubjectId(subjectId);
            existing.setResourceType(resourceType);
            existing.setResourceId(resourceId);
            existing.setPermission(permission);
            existing.setUpdateTime(System.currentTimeMillis());
            resourceAclProfile.update(existing);
            logService.log(LogType.UserLog.UPDATE,
                    String.format("[%s]更新资源授权 %s/%s -> %s/%s [%s]",
                            currentUserName, subjectType, subjectId, resourceType, resourceId, permission));
            return existing.getId();
        }

        // 新增：支持多类型批量，payload 形如 group:id1,id2|connector:id3
        List<String[]> bindings = parseResourceBindings(params);
        Assert.isTrue(!CollectionUtils.isEmpty(bindings), "请选择资源");
        int created = 0;
        int updated = 0;
        for (String[] binding : bindings) {
            String resourceType = binding[0];
            String resourceId = binding[1];
            assertResourceExists(resourceType, resourceId);
            if (upsertAcl(subjectType, subjectId, resourceType, resourceId, permission)) {
                created++;
            } else {
                updated++;
            }
        }
        logService.log(LogType.UserLog.INSERT,
                String.format("[%s]批量资源授权 %s/%s -> %d 项 (新增%d/覆盖%d) [%s]",
                        currentUserName, subjectType, subjectId, bindings.size(), created, updated, permission));
        return String.format("成功授权 %d 项（新增 %d，覆盖 %d）", bindings.size(), created, updated);
    }

    @Override
    public synchronized void grantOwner(String username, String resourceType, String resourceId) {
        if (StringUtil.isBlank(username) || StringUtil.isBlank(resourceType) || StringUtil.isBlank(resourceId)) {
            return;
        }
        if (permissionService.isAdmin(username)) {
            return;
        }
        if (!ResourceTypeEnum.isLeaf(resourceType)) {
            return;
        }
        UserInfo user = userProfile.getByUsername(username);
        if (user == null || StringUtil.isBlank(user.getId())) {
            return;
        }
        upsertAcl(ResourceAclSubjectEnum.USER.getCode(), user.getId(), resourceType, resourceId,
                ResourceAclPermissionEnum.READ_WRITE.getCode());
    }

    /**
     * 按唯一键新增或覆盖权限。
     *
     * @return true 新增，false 覆盖
     */
    private boolean upsertAcl(int subjectType, String subjectId, String resourceType, String resourceId, int permission) {
        long now = System.currentTimeMillis();
        ResourceAcl existing = resourceAclProfile.getByUnique(subjectType, subjectId, resourceType, resourceId);
        if (existing != null) {
            existing.setPermission(permission);
            existing.setUpdateTime(now);
            resourceAclProfile.update(existing);
            return false;
        }
        ResourceAcl acl = new ResourceAcl();
        acl.setSubjectType(subjectType);
        acl.setSubjectId(subjectId);
        acl.setResourceType(resourceType);
        acl.setResourceId(resourceId);
        acl.setPermission(permission);
        acl.setCreateTime(now);
        acl.setUpdateTime(now);
        resourceAclProfile.add(acl);
        return true;
    }

    /**
     * 解析批量资源：优先 resourcePayload（type:id,...|type:id），否则单类型 resourceType + resourceIds。
     * 每项为 [resourceType, resourceId]。
     */
    private List<String[]> parseResourceBindings(Map<String, String> params) {
        List<String[]> bindings = new ArrayList<>();
        String payload = params.get("resourcePayload");
        if (StringUtil.isNotBlank(payload)) {
            for (String segment : payload.split("\\|")) {
                if (StringUtil.isBlank(segment) || !segment.contains(":")) {
                    continue;
                }
                int idx = segment.indexOf(':');
                String type = normalizeResourceType(segment.substring(0, idx));
                for (String resourceId : parseResourceIds(segment.substring(idx + 1), null)) {
                    bindings.add(new String[]{type, resourceId});
                }
            }
            return bindings;
        }
        String resourceType = normalizeResourceType(params.get("resourceType"));
        for (String resourceId : parseResourceIds(params.get("resourceIds"), params.get("resourceId"))) {
            bindings.add(new String[]{resourceType, resourceId});
        }
        return bindings;
    }

    private List<String> parseResourceIds(String resourceIds, String resourceId) {
        String raw = resourceIds;
        if (StringUtil.isBlank(raw)) {
            raw = resourceId;
        }
        if (StringUtil.isBlank(raw)) {
            return Collections.emptyList();
        }
        return Arrays.stream(raw.split(StringUtil.COMMA))
                .map(StringUtil::trim)
                .filter(StringUtil::isNotBlank)
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public synchronized String remove(Map<String, String> params) {
        Assert.notNull(params, "参数不能为空");
        String currentUserName = params.get(UserConfigService.CURRENT_USER_NAME);
        permissionChecker.requirePermission(currentUserName, PERM_ACL_EDIT);
        String id = params.get("id");
        Assert.hasText(id, "授权ID不能为空");
        ResourceAcl acl = resourceAclProfile.get(id);
        Assert.notNull(acl, "授权不存在");
        resourceAclProfile.remove(id);
        logService.log(LogType.UserLog.DELETE,
                String.format("[%s]删除资源授权 %s/%s -> %s/%s",
                        currentUserName, acl.getSubjectType(), acl.getSubjectId(), acl.getResourceType(), acl.getResourceId()));
        return "删除成功";
    }

    @Override
    public void revokeAllForResource(String resourceType, String resourceId) {
        if (StringUtil.isBlank(resourceType) || StringUtil.isBlank(resourceId)) {
            return;
        }
        resourceAclProfile.removeByResource(resourceType, resourceId);
    }

    @Override
    public void revokeAllForSubject(int subjectType, String subjectId) {
        if (StringUtil.isBlank(subjectId)) {
            return;
        }
        resourceAclProfile.removeBySubject(subjectType, subjectId);
    }

    @Override
    public boolean check(String username, String resourceType, String resourceId, String action) {
        if (StringUtil.isBlank(username) || StringUtil.isBlank(resourceType)
                || StringUtil.isBlank(resourceId) || StringUtil.isBlank(action)) {
            return false;
        }
        if (permissionService.isAdmin(username)) {
            return true;
        }
        UserInfo user = userProfile.getByUsername(username);
        if (user == null || StringUtil.isBlank(user.getId())) {
            return false;
        }
        String need = action.trim().toLowerCase(Locale.ROOT);
        if (hasAction(resourceAclProfile.listBySubject(ResourceAclSubjectEnum.USER.getCode(), user.getId()),
                resourceType, resourceId, need)) {
            return true;
        }
        List<UserRole> userRoles = rbacProfile.listUserRolesByUserId(user.getId());
        if (!CollectionUtils.isEmpty(userRoles)) {
            for (UserRole ur : userRoles) {
                if (ur == null || StringUtil.isBlank(ur.getRoleId())) {
                    continue;
                }
                if (hasAction(resourceAclProfile.listBySubject(ResourceAclSubjectEnum.ROLE.getCode(), ur.getRoleId()),
                        resourceType, resourceId, need)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void require(String username, String resourceType, String resourceId, String action) {
        Assert.hasText(username, "无法获取登录用户.");
        Assert.isTrue(check(username, resourceType, resourceId, action), "无资源权限");
    }

    @Override
    public Set<String> resolveAllowedIds(String username, String resourceType, String action) {
        if (StringUtil.isBlank(username) || StringUtil.isBlank(resourceType) || StringUtil.isBlank(action)) {
            return Collections.emptySet();
        }
        if (permissionService.isAdmin(username)) {
            return null;
        }
        UserInfo user = userProfile.getByUsername(username);
        if (user == null || StringUtil.isBlank(user.getId())) {
            return Collections.emptySet();
        }
        Set<String> allowed = new LinkedHashSet<>();
        collectAllowedIds(resourceAclProfile.listBySubject(ResourceAclSubjectEnum.USER.getCode(), user.getId()), resourceType, action, allowed);
        List<UserRole> userRoles = rbacProfile.listUserRolesByUserId(user.getId());
        if (!CollectionUtils.isEmpty(userRoles)) {
            for (UserRole ur : userRoles) {
                collectAllowedIds(resourceAclProfile.listBySubject(ResourceAclSubjectEnum.ROLE.getCode(), ur.getRoleId()),
                        resourceType, action, allowed);
            }
        }
        return allowed;
    }

    @Override
    public GroupSearchFilter applyToSearchFilter(GroupSearchFilter groupFilter, Set<String> allowedIds) {
        GroupSearchFilter base = groupFilter == null ? GroupSearchFilter.NONE : groupFilter;
        if (allowedIds == null) {
            return base;
        }
        if (allowedIds.isEmpty()) {
            return GroupSearchFilter.EMPTY;
        }
        if (base.isEmptyResult()) {
            return GroupSearchFilter.EMPTY;
        }
        if (!base.hasIdFilter()) {
            return new GroupSearchFilter(false, new ArrayList<>(allowedIds));
        }
        List<String> intersected = base.getIds().stream()
                .filter(allowedIds::contains)
                .collect(Collectors.toList());
        if (intersected.isEmpty()) {
            return GroupSearchFilter.EMPTY;
        }
        return new GroupSearchFilter(false, intersected);
    }

    @Override
    public <T> List<T> filter(String username, String resourceType, List<T> list,
                              Function<T, String> idGetter, String action) {
        if (CollectionUtils.isEmpty(list) || idGetter == null) {
            return list == null ? Collections.emptyList() : list;
        }
        Set<String> allowed = resolveAllowedIds(username, resourceType, action);
        if (allowed == null) {
            return list;
        }
        if (allowed.isEmpty()) {
            return Collections.emptyList();
        }
        List<T> result = new ArrayList<>(list.size());
        for (T item : list) {
            if (item == null) {
                continue;
            }
            String id = idGetter.apply(item);
            if (StringUtil.isNotBlank(id) && allowed.contains(id)) {
                result.add(item);
            }
        }
        return result;
    }

    private void collectAllowedIds(List<ResourceAcl> resourceIds, String resourceType, String need, Set<String> out) {
        if (CollectionUtils.isEmpty(resourceIds) || out == null) {
            return;
        }
        String action = StringUtil.isBlank(need) ? StringUtil.EMPTY : need.trim().toLowerCase(Locale.ROOT);
        for (ResourceAcl resource : resourceIds) {
            if (resource == null || !ResourceAclPermissionEnum.satisfies(resource.getPermission(), action)) {
                continue;
            }
            if (StringUtil.equals(resourceType, resource.getResourceType())) {
                out.add(resource.getResourceId());
                continue;
            }
            if (StringUtil.equals(ResourceTypeEnum.GROUP.getCode(), resource.getResourceType()) && StringUtil.isNotBlank(resource.getResourceId())) {
                List<String> memberIds = groupRelProfile.listResourceIds(resource.getResourceId(), resourceType);
                if (!CollectionUtils.isEmpty(memberIds)) {
                    out.addAll(memberIds);
                }
            }
        }
    }

    private boolean hasAction(List<ResourceAcl> acls, String resourceType, String resourceId, String need) {
        if (CollectionUtils.isEmpty(acls)) {
            return false;
        }
        for (ResourceAcl acl : acls) {
            if (acl == null || !ResourceAclPermissionEnum.satisfies(acl.getPermission(), need)) {
                continue;
            }
            if (StringUtil.equals(resourceType, acl.getResourceType())
                    && StringUtil.equals(resourceId, acl.getResourceId())) {
                return true;
            }
            if (ResourceTypeEnum.GROUP.getCode().equals(acl.getResourceType()) && StringUtil.isNotBlank(acl.getResourceId())) {
                List<GroupRel> rels = groupRelProfile.listByResource(resourceType, resourceId);
                if (!CollectionUtils.isEmpty(rels)) {
                    for (GroupRel rel : rels) {
                        if (rel != null && StringUtil.equals(acl.getResourceId(), rel.getGroupId())) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    private ResourceAclVO toResourceAclVO(ResourceAcl acl) {
        ResourceAclVO vo = new ResourceAclVO();
        BeanUtils.copyProperties(acl, vo);
        ResourceAclSubjectEnum subject = ResourceAclSubjectEnum.get(acl.getSubjectType());
        vo.setSubjectTypeName(subject == null ? String.valueOf(acl.getSubjectType()) : subject.getName());
        vo.setSubjectName(resolveSubjectName(acl.getSubjectType(), acl.getSubjectId()));
        vo.setResourceName(resolveResourceName(acl.getResourceType(), acl.getResourceId()));
        ResourceAclPermissionEnum permission = ResourceAclPermissionEnum.get(acl.getPermission());
        vo.setPermissionName(permission == null ? String.valueOf(acl.getPermission()) : permission.getName());
        return vo;
    }

    private String resolveSubjectName(int subjectType, String subjectId) {
        ResourceAclSubjectEnum subject = ResourceAclSubjectEnum.get(subjectType);
        if (subject == null) {
            return subjectId;
        }
        if (subject.isRole()) {
            Role role = rbacProfile.getRoleById(subjectId);
            return role == null ? subjectId : role.getName();
        }
        List<UserInfo> users = userProfile.listUserInfos();
        if (!CollectionUtils.isEmpty(users)) {
            for (UserInfo user : users) {
                if (user != null && StringUtil.equals(subjectId, user.getId())) {
                    return user.getNickname() + " (" + user.getUsername() + ")";
                }
            }
        }
        return subjectId;
    }

    private String resolveResourceName(String resourceType, String resourceId) {
        ResourceTypeEnum type = ResourceTypeEnum.get(resourceType);
        if (type == null) {
            return resourceId;
        }
        switch (type) {
            case GROUP:
                Group group = groupProfile.getGroup(resourceId);
                return group == null ? resourceId : group.getName();
            case CONNECTOR:
                Connector connector = connectorProfile.getConnector(resourceId);
                return connector == null ? resourceId : connector.getName();
            case MAPPING:
                Mapping mapping = mappingProfile.get(resourceId);
                return mapping == null ? resourceId : mapping.getName();
            case VALIDATE_SYNC:
                ValidateSyncTask validateSyncTask = validateSyncProfile.get(resourceId);
                return validateSyncTask == null ? resourceId : validateSyncTask.getName();
            case DATABASE_SYNC:
                DatabaseSyncTask databaseSyncTask = databaseSyncProfile.get(resourceId);
                return databaseSyncTask == null ? resourceId : databaseSyncTask.getName();
            default:
                return resourceId;
        }
    }

    private String normalizeResourceType(String raw) {
        String type = StringUtil.isBlank(raw) ? ResourceTypeEnum.GROUP.getCode() : raw.trim();
        Assert.isTrue(ResourceTypeEnum.isSupported(type), "不支持的资源类型: " + type);
        return type;
    }

    private void assertResourceExists(String resourceType, String resourceId) {
        ResourceTypeEnum type = ResourceTypeEnum.get(resourceType);
        Assert.notNull(type, "不支持的资源类型: " + resourceType);
        switch (type) {
            case GROUP:
                Assert.notNull(groupProfile.getGroup(resourceId), "分组不存在");
                return;
            case CONNECTOR:
                Assert.notNull(connectorProfile.getConnector(resourceId), "连接不存在");
                return;
            case MAPPING:
                Assert.notNull(mappingProfile.get(resourceId), "同步任务不存在");
                return;
            case VALIDATE_SYNC:
                Assert.notNull(validateSyncProfile.get(resourceId), "订正校验任务不存在");
                return;
            case DATABASE_SYNC:
                Assert.notNull(databaseSyncProfile.get(resourceId), "整库迁移任务不存在");
                return;
            default:
                throw new IllegalArgumentException("不支持的资源类型: " + resourceType);
        }
    }

    private String resolveSubjectId(int subjectType, String subjectId, String username) {
        ResourceAclSubjectEnum subject = ResourceAclSubjectEnum.get(subjectType);
        Assert.notNull(subject, "主体类型仅支持用户/角色");
        if (StringUtil.isNotBlank(subjectId)) {
            if (subject.isRole()) {
                Assert.notNull(rbacProfile.getRoleById(subjectId), "角色不存在");
            } else {
                Assert.notNull(userProfile.getById(subjectId), "用户不存在");
            }
            return subjectId;
        }
        if (subject.isRole()) {
            throw new IllegalArgumentException("请选择角色");
        }
        Assert.hasText(username, "请选择用户");
        UserInfo user = userProfile.getByUsername(username.trim());
        Assert.notNull(user, "用户不存在");
        Assert.hasText(user.getId(), "用户ID无效");
        return user.getId();
    }

    private int normalizeSubjectType(String raw) {
        Assert.hasText(raw, "主体类型不能为空");
        Assert.isTrue(NumberUtil.isCreatable(raw.trim()), "主体类型无效");
        int code = NumberUtil.toInt(raw.trim());
        Assert.isTrue(ResourceAclSubjectEnum.isSupported(code), "主体类型仅支持用户/角色");
        return code;
    }

    private int normalizePermission(String raw) {
        Assert.hasText(raw, "请选择权限");
        Assert.isTrue(NumberUtil.isCreatable(raw.trim()), "权限无效");
        int code = NumberUtil.toInt(raw.trim());
        Assert.isTrue(ResourceAclPermissionEnum.isSupported(code), "权限仅支持只读/可读写");
        return code;
    }
}
