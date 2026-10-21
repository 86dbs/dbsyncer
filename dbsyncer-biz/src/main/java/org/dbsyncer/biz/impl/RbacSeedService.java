/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;

/**
 * RBAC 一期种子：按 CODE 幂等灌入内置角色/权限/绑权，并按用户 ROLE 列同步 user_role。
 * 不改变现网鉴权行为（仍读 dbsyncer_user.ROLE）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
@Component
@Order(Ordered.LOWEST_PRECEDENCE)
public class RbacSeedService implements ApplicationListener<ContextRefreshedEvent> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String ROLE_ADMIN = "admin";
    private static final String ROLE_USER = "user";

    private static final String KIND_MENU = "MENU";
    private static final String KIND_ACTION = "ACTION";

    @Resource
    private RbacProfile rbacProfile;

    @Resource
    private UserProfile userProfile;

    private volatile boolean seeded;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if (event.getApplicationContext().getParent() != null || seeded) {
            return;
        }
        seeded = true;
        try {
            seed();
        } catch (Exception e) {
            logger.error("RBAC 种子初始化失败", e);
        }
    }

    private void seed() {
        Role adminRole = ensureRole(ROLE_ADMIN, "管理员", 1, "内置管理员");
        Role userRole = ensureRole(ROLE_USER, "普通用户", 1, "内置普通用户");

        List<PermissionDef> defs = permissionDefs();
        for (PermissionDef def : defs) {
            Permission permission = ensurePermission(def);
            ensureRolePermission(adminRole.getId(), permission.getId());
        }

        List<String> userCodes = Arrays.asList(
                "connector:view", "mapping:view", "mapping:operate",
                "notice:view", "license:view", "system:view"
        );
        for (String code : userCodes) {
            Permission permission = rbacProfile.getPermissionByCode(code);
            if (permission != null) {
                ensureRolePermission(userRole.getId(), permission.getId());
            }
        }

        List<UserInfo> users = userProfile.listUserInfos();
        if (CollectionUtils.isEmpty(users)) {
            logger.info("RBAC 种子完成：角色/权限已就绪，暂无用户可绑角色");
            return;
        }
        for (UserInfo user : users) {
            if (user == null || StringUtil.isBlank(user.getId())) {
                continue;
            }
            String roleCode = StringUtil.equals(ROLE_ADMIN, user.getRoleCode()) ? ROLE_ADMIN : ROLE_USER;
            Role role = ROLE_ADMIN.equals(roleCode) ? adminRole : userRole;
            ensureUserRole(user.getId(), role.getId());
        }
        logger.info("RBAC 种子完成：roles=2, permissions={}, users={}", defs.size(), users.size());
    }

    private Role ensureRole(String code, String name, int builtin, String remark) {
        Role existing = rbacProfile.getRoleByCode(code);
        if (existing != null) {
            return existing;
        }
        long now = System.currentTimeMillis();
        Role role = new Role();
        role.setType(ConfigConstant.ROLE);
        role.setCode(code);
        role.setName(name);
        role.setBuiltin(builtin);
        role.setRemark(remark);
        role.setCreateTime(now);
        role.setUpdateTime(now);
        rbacProfile.addRole(role);
        return rbacProfile.getRoleByCode(code);
    }

    private Permission ensurePermission(PermissionDef def) {
        Permission existing = rbacProfile.getPermissionByCode(def.code);
        if (existing != null) {
            return existing;
        }
        long now = System.currentTimeMillis();
        Permission permission = new Permission();
        permission.setType(ConfigConstant.PERMISSION);
        permission.setCode(def.code);
        permission.setName(def.name);
        permission.setModule(def.module);
        permission.setKind(def.kind);
        permission.setRemark("");
        permission.setCreateTime(now);
        permission.setUpdateTime(now);
        rbacProfile.addPermission(permission);
        return rbacProfile.getPermissionByCode(def.code);
    }

    private void ensureRolePermission(String roleId, String permissionId) {
        if (rbacProfile.getRolePermission(roleId, permissionId) != null) {
            return;
        }
        long now = System.currentTimeMillis();
        RolePermission rel = new RolePermission();
        rel.setType(ConfigConstant.ROLE_PERMISSION);
        rel.setRoleId(roleId);
        rel.setPermissionId(permissionId);
        rel.setCreateTime(now);
        rel.setUpdateTime(now);
        rbacProfile.addRolePermission(rel);
    }

    private void ensureUserRole(String userId, String roleId) {
        if (rbacProfile.getUserRole(userId, roleId) != null) {
            return;
        }
        long now = System.currentTimeMillis();
        UserRole rel = new UserRole();
        rel.setType(ConfigConstant.USER_ROLE_REL);
        rel.setUserId(userId);
        rel.setRoleId(roleId);
        rel.setCreateTime(now);
        rel.setUpdateTime(now);
        rbacProfile.addUserRole(rel);
    }

    private static List<PermissionDef> permissionDefs() {
        return Arrays.asList(
                new PermissionDef("user:view", "查看用户", "user", KIND_MENU),
                new PermissionDef("user:edit", "编辑用户", "user", KIND_ACTION),
                new PermissionDef("connector:view", "查看连接器", "connector", KIND_MENU),
                new PermissionDef("connector:edit", "编辑连接器", "connector", KIND_ACTION),
                new PermissionDef("mapping:view", "查看同步任务", "mapping", KIND_MENU),
                new PermissionDef("mapping:edit", "编辑同步任务", "mapping", KIND_ACTION),
                new PermissionDef("mapping:operate", "启停同步任务", "mapping", KIND_ACTION),
                new PermissionDef("notice:view", "查看告警配置", "notice", KIND_MENU),
                new PermissionDef("notice:edit", "编辑告警配置", "notice", KIND_ACTION),
                new PermissionDef("license:view", "查看授权", "license", KIND_MENU),
                new PermissionDef("license:edit", "管理授权", "license", KIND_ACTION),
                new PermissionDef("system:view", "查看系统配置", "system", KIND_MENU),
                new PermissionDef("system:edit", "编辑系统配置", "system", KIND_ACTION)
        );
    }

    private static final class PermissionDef {
        private final String code;
        private final String name;
        private final String module;
        private final String kind;

        private PermissionDef(String code, String name, String module, String kind) {
            this.code = code;
            this.name = name;
            this.module = module;
            this.kind = kind;
        }
    }
}
