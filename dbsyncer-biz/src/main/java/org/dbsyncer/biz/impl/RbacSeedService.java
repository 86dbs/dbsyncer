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
        // 仅内置超级管理员；历史「普通用户」角色若存在则降为非内置（可删可改）
        Role adminRole = ensureRole(ROLE_ADMIN, "超级管理员", 1, "内置超级管理员");
        demoteLegacyUserRoleIfPresent();

        List<PermissionDef> defs = permissionDefs();
        for (PermissionDef def : defs) {
            Permission permission = ensurePermission(def);
            ensureRolePermission(adminRole.getId(), permission.getId());
        }

        List<UserInfo> users = userProfile.listUserInfos();
        if (CollectionUtils.isEmpty(users)) {
            logger.info("RBAC 种子完成：内置角色=超级管理员，permissions={}，暂无用户可绑角色", defs.size());
            return;
        }
        int bound = 0;
        for (UserInfo user : users) {
            if (user == null || StringUtil.isBlank(user.getId())) {
                continue;
            }
            // 仅将 ROLE=admin 的用户绑到超级管理员；其余由管理员在用户管理中指定角色
            if (StringUtil.equals(ROLE_ADMIN, user.getRoleCode())) {
                ensureUserRole(user.getId(), adminRole.getId());
                bound++;
            }
        }
        logger.info("RBAC 种子完成：内置角色=超级管理员，permissions={}，users={}，admin绑定={}",
                defs.size(), users.size(), bound);
    }

    /**
     * 历史种子曾内置 user 角色；现仅保留超级管理员为内置，将 user 降为自定义。
     */
    private void demoteLegacyUserRoleIfPresent() {
        Role userRole = rbacProfile.getRoleByCode(ROLE_USER);
        if (userRole == null || userRole.getBuiltin() != 1) {
            return;
        }
        userRole.setBuiltin(0);
        if (StringUtil.isBlank(userRole.getRemark()) || StringUtil.equals("内置普通用户", userRole.getRemark())) {
            userRole.setRemark("自定义角色（由历史内置普通用户降级）");
        }
        userRole.setUpdateTime(System.currentTimeMillis());
        rbacProfile.updateRole(userRole);
        logger.info("已将历史内置角色 user 降为非内置");
    }

    private Role ensureRole(String code, String name, int builtin, String remark) {
        Role existing = rbacProfile.getRoleByCode(code);
        if (existing != null) {
            // 同步内置超级管理员展示名
            if (ROLE_ADMIN.equals(code) && (existing.getBuiltin() != 1
                    || !StringUtil.equals(name, existing.getName()))) {
                existing.setBuiltin(1);
                existing.setName(name);
                existing.setRemark(remark);
                existing.setUpdateTime(System.currentTimeMillis());
                rbacProfile.updateRole(existing);
            }
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
                new PermissionDef("system:edit", "编辑系统配置", "system", KIND_ACTION),
                new PermissionDef("acl:view", "查看资源授权", "acl", KIND_MENU),
                new PermissionDef("acl:edit", "编辑资源授权", "acl", KIND_ACTION)
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
