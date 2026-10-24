/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.PermissionService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.RbacProfile;
import org.dbsyncer.parser.UserProfile;
import org.dbsyncer.parser.model.Permission;
import org.dbsyncer.parser.model.Role;
import org.dbsyncer.parser.model.RolePermission;
import org.dbsyncer.parser.model.UserInfo;
import org.dbsyncer.parser.model.UserRole;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link PermissionService} 实现。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
@Service
public class PermissionServiceImpl implements PermissionService {

    @Resource
    private RbacProfile rbacProfile;

    @Resource
    private UserProfile userProfile;

    @Override
    public boolean isAdmin(String username) {
        return StringUtil.isNotBlank(username)
                && StringUtil.equals(UserConfigServiceImpl.DEFAULT_USERNAME, username);
    }

    @Override
    public boolean hasPermission(String username, String permissionCode) {
        if (StringUtil.isBlank(permissionCode)) {
            return false;
        }
        if (isAdmin(username)) {
            return true;
        }
        return listPermissionCodes(username).contains(permissionCode);
    }

    @Override
    public Set<String> listPermissionCodes(String username) {
        UserInfo user = userProfile.getByUsername(username);
        if (user == null) {
            return Collections.emptySet();
        }
        List<UserRole> userRoles = rbacProfile.listUserRolesByUserId(user.getId());
        if (CollectionUtils.isEmpty(userRoles)) {
            return Collections.emptySet();
        }
        //获取用户角色
        List<String> roleIds = userRoles.stream()
                .filter(ur -> ur != null && StringUtil.isNotBlank(ur.getRoleId()))
                .map(UserRole::getRoleId)
                .distinct()
                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(roleIds)) {
            return Collections.emptySet();
        }
        //获取角色code语句
        List<RolePermission> rolePermissions = rbacProfile.listRolePermissionsByRoleIds(roleIds);
        if (CollectionUtils.isEmpty(rolePermissions)) {
            return Collections.emptySet();
        }
        List<String> permissionIds = rolePermissions.stream()
                .filter(rp -> rp != null && StringUtil.isNotBlank(rp.getPermissionId()))
                .map(RolePermission::getPermissionId)
                .distinct()
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(permissionIds)) {
            return Collections.emptySet();
        }
        Set<String> codes = new LinkedHashSet<>();

        //获取菜单code
        for (Permission permission : rbacProfile.listPermissionsByIds(permissionIds)) {
            if (permission != null && StringUtil.isNotBlank(permission.getCode())) {
                codes.add(permission.getCode());
            }
        }
        return codes;
    }

    @Override
    public String buildAuthorityString(String username) {
        Set<String> authorities = new LinkedHashSet<>(listPermissionCodes(username));
        if (isAdmin(username)) {
            authorities.add(AUTHORITY_ROLE_ADMIN);
        }
        return authorities.stream().collect(Collectors.joining(","));
    }

    @Override
    public void bindUserRole(String userId, String roleId) {
        if (StringUtil.isBlank(userId) || StringUtil.isBlank(roleId)) {
            return;
        }
        Role role = rbacProfile.getRoleById(roleId);
        if (role == null) {
            return;
        }
        if (rbacProfile.getUserRole(userId, role.getId()) != null) {
            return;
        }
        long now = System.currentTimeMillis();
        UserRole rel = new UserRole();
        rel.setType(ConfigConstant.USER_ROLE_REL);
        rel.setUserId(userId);
        rel.setRoleId(role.getId());
        rel.setCreateTime(now);
        rel.setUpdateTime(now);
        rbacProfile.addUserRole(rel);
    }

    @Override
    public void replaceUserRole(String userId, String roleId) {
        if (StringUtil.isBlank(userId) || StringUtil.isBlank(roleId)) {
            return;
        }
        unbindAllUserRoles(userId);
        bindUserRole(userId, roleId);
    }

    @Override
    public void unbindAllUserRoles(String userId) {
        rbacProfile.removeUserRolesByUserId(userId);
    }

    @Override
    public String resolvePrimaryRoleId(String userId) {
        Role role = resolvePrimaryRole(userId);
        return role == null ? null : role.getId();
    }

    @Override
    public String resolvePrimaryRoleName(String userId) {
        Role role = resolvePrimaryRole(userId);
        return role == null ? null : role.getName();
    }

    private Role resolvePrimaryRole(String userId) {
        if (StringUtil.isBlank(userId)) {
            return null;
        }
        List<UserRole> userRoles = rbacProfile.listUserRolesByUserId(userId);
        if (CollectionUtils.isEmpty(userRoles)) {
            return null;
        }
        for (UserRole rel : userRoles) {
            if (rel == null || StringUtil.isBlank(rel.getRoleId())) {
                continue;
            }
            Role role = rbacProfile.getRoleById(rel.getRoleId());
            if (role != null) {
                return role;
            }
        }
        return null;
    }
}
