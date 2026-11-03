/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import java.util.Set;

/**
 * 功能权限查询（一期 RBAC）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
public interface PermissionService {

    /**
     * 超管时写入 Authority 的标记。
     */
    String AUTHORITY_ROLE_ADMIN = "role_admin";

    /**
     * 是否超管（仅 username 为默认账号 admin）。
     */
    boolean isAdmin(String username);

    /**
     * 是否拥有指定权限码（如 {@code mapping:operate}）。
     */
    boolean hasPermission(String username, String permissionCode);

    /**
     * 用户全部权限码（多角色并集）。
     */
    Set<String> listPermissionCodes(String username);

    /**
     * 登录/SSO 用：权限码 + 可选 {@link #AUTHORITY_ROLE_ADMIN}，逗号拼接。
     */
    String buildAuthorityString(String username);

    /**
     * 为用户绑定角色（按角色 ID，幂等）。
     */
    void bindUserRole(String userId, String roleId);

    /**
     * 覆盖用户角色绑定（先清空再绑一个）。
     */
    void replaceUserRole(String userId, String roleId);

    /**
     * 删除用户全部角色绑定。
     */
    void unbindAllUserRoles(String userId);

    /**
     * 用户主角色 ID（取第一条 user_role）。
     */
    String resolvePrimaryRoleId(String userId);

    /**
     * 用户主角色名称。
     */
    String resolvePrimaryRoleName(String userId);
}
