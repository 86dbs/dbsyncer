/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import java.util.List;
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
     * Spring Security 角色前缀：拥有 admin 角色时写入 Authority。
     */
    String AUTHORITY_ROLE_ADMIN = "ROLE_admin";

    /**
     * 用户是否具备管理员角色（role.code=admin）。
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
     * 为用户绑定角色（按角色编码，幂等）。
     */
    void bindUserRole(String userId, String roleCode);

    /**
     * 删除用户全部角色绑定。
     */
    void unbindAllUserRoles(String userId);
}
