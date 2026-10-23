/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.PermissionService;
import org.dbsyncer.biz.enums.UserRoleEnum;
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

    private static final String ROLE_ADMIN = "admin";

    @Resource
    private RbacProfile rbacProfile;

    @Resource
    private UserProfile userProfile;

    @Override
    public boolean isAdmin(String username) {
        if (StringUtil.isBlank(username)) {
            return false;
        }
        UserInfo user = userProfile.getByUsername(username);
        if (user == null) {
            return false;
        }
        List<UserRole> userRoles = rbacProfile.listUserRolesByUserId(user.getId());
        if (CollectionUtils.isEmpty(userRoles)) {
            return false;
        }
        for (UserRole ur : userRoles) {
            Role role = rbacProfile.getRoleById(ur.getRoleId());
            if (role != null && ROLE_ADMIN.equals(role.getCode())) {
                return true;
            }
        }
        return false;
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
        Set<String> codes = new LinkedHashSet<>();
        List<UserRole> userRoles = rbacProfile.listUserRolesByUserId(user.getId());
        if (CollectionUtils.isEmpty(userRoles)) {
            return codes;
        }
        for (UserRole ur : userRoles) {
            List<RolePermission> rolePermissions = rbacProfile.listRolePermissionsByRoleId(ur.getRoleId());
            if (CollectionUtils.isEmpty(rolePermissions)) {
                continue;
            }
            for (RolePermission rp : rolePermissions) {
                Permission permission = rbacProfile.getPermissionById(rp.getPermissionId());
                if (permission != null && StringUtil.isNotBlank(permission.getCode())) {
                    codes.add(permission.getCode());
                }
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
    public void bindUserRole(String userId, String roleCode) {
        if (StringUtil.isBlank(userId) || StringUtil.isBlank(roleCode)) {
            return;
        }
        Role role = rbacProfile.getRoleByCode(roleCode);
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
    public void replaceUserRole(String userId, String roleCode) {
        if (StringUtil.isBlank(userId) || StringUtil.isBlank(roleCode)) {
            return;
        }
        unbindAllUserRoles(userId);
        bindUserRole(userId, roleCode);
    }

    @Override
    public void unbindAllUserRoles(String userId) {
        rbacProfile.removeUserRolesByUserId(userId);
    }

    @Override
    public String resolvePrimaryRoleCode(String userId, String fallbackRoleCode) {
        Role role = resolvePrimaryRole(userId);
        if (role != null && StringUtil.isNotBlank(role.getCode())) {
            return role.getCode();
        }
        return fallbackRoleCode;
    }

    @Override
    public String resolvePrimaryRoleName(String userId, String fallbackRoleCode) {
        Role role = resolvePrimaryRole(userId);
        if (role != null && StringUtil.isNotBlank(role.getName())) {
            return role.getName();
        }
        String name = UserRoleEnum.getNameByCode(fallbackRoleCode);
        return StringUtil.isNotBlank(name) ? name : fallbackRoleCode;
    }

    @Override
    public int countUsersByRoleCode(String roleCode) {
        if (StringUtil.isBlank(roleCode)) {
            return 0;
        }
        Role role = rbacProfile.getRoleByCode(roleCode);
        if (role == null || StringUtil.isBlank(role.getId())) {
            return 0;
        }
        List<UserRole> userRoles = rbacProfile.listUserRolesByRoleId(role.getId());
        return CollectionUtils.isEmpty(userRoles) ? 0 : userRoles.size();
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
