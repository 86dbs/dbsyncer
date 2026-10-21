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
    private static final String ROLE_USER = "user";

    @Resource
    private RbacProfile rbacProfile;

    @Resource
    private UserProfile userProfile;

    @Override
    public boolean isAdmin(String username) {
        if (StringUtil.isBlank(username)) {
            return false;
        }
        UserInfo user = findUser(username);
        if (user == null) {
            return false;
        }
        List<UserRole> userRoles = rbacProfile.listUserRolesByUserId(user.getId());
        if (!CollectionUtils.isEmpty(userRoles)) {
            for (UserRole ur : userRoles) {
                Role role = rbacProfile.getRoleById(ur.getRoleId());
                if (role != null && ROLE_ADMIN.equals(role.getCode())) {
                    return true;
                }
            }
        }
        // 兼容：尚未灌入 user_role 时回退旧 ROLE 列
        return UserRoleEnum.isAdmin(user.getRoleCode());
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
        UserInfo user = findUser(username);
        if (user == null) {
            return Collections.emptySet();
        }
        Set<String> codes = new LinkedHashSet<>();
        List<UserRole> userRoles = rbacProfile.listUserRolesByUserId(user.getId());
        if (!CollectionUtils.isEmpty(userRoles)) {
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
        // 兼容回退：按旧 ROLE 列给默认权限集合
        if (UserRoleEnum.isAdmin(user.getRoleCode())) {
            List<Permission> all = rbacProfile.listPermissions();
            if (!CollectionUtils.isEmpty(all)) {
                for (Permission p : all) {
                    if (p != null && StringUtil.isNotBlank(p.getCode())) {
                        codes.add(p.getCode());
                    }
                }
            }
            return codes;
        }
        Collections.addAll(codes,
                "connector:view", "mapping:view", "mapping:operate",
                "notice:view", "license:view", "system:view");
        return codes;
    }

    @Override
    public String buildAuthorityString(String username) {
        Set<String> authorities = new LinkedHashSet<>(listPermissionCodes(username));
        if (isAdmin(username)) {
            authorities.add(AUTHORITY_ROLE_ADMIN);
        }
        if (authorities.isEmpty()) {
            // 保底：至少带上旧 roleCode，避免空 Authority
            UserInfo user = findUser(username);
            if (user != null && StringUtil.isNotBlank(user.getRoleCode())) {
                return user.getRoleCode();
            }
            return StringUtil.EMPTY;
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
            role = rbacProfile.getRoleByCode(ROLE_USER.equals(roleCode) ? ROLE_USER : ROLE_ADMIN);
        }
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
    public void unbindAllUserRoles(String userId) {
        rbacProfile.removeUserRolesByUserId(userId);
    }

    private UserInfo findUser(String username) {
        List<UserInfo> users = userProfile.listUserInfos();
        if (CollectionUtils.isEmpty(users)) {
            return null;
        }
        for (UserInfo user : users) {
            if (user != null && StringUtil.equals(username, user.getUsername())) {
                return user;
            }
        }
        return null;
    }
}
