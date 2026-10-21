/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.Permission;
import org.dbsyncer.parser.model.Role;
import org.dbsyncer.parser.model.RolePermission;
import org.dbsyncer.parser.model.UserRole;

import java.util.List;

/**
 * RBAC 读写（角色 / 权限点 / 关系）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
public interface RbacProfile {

    Role getRoleByCode(String code);

    List<Role> listRoles();

    String addRole(Role role);

    String updateRole(Role role);

    Permission getPermissionByCode(String code);

    List<Permission> listPermissions();

    String addPermission(Permission permission);

    String updatePermission(Permission permission);

    RolePermission getRolePermission(String roleId, String permissionId);

    List<RolePermission> listRolePermissionsByRoleId(String roleId);

    String addRolePermission(RolePermission rel);

    UserRole getUserRole(String userId, String roleId);

    List<UserRole> listUserRolesByUserId(String userId);

    String addUserRole(UserRole rel);

    /**
     * 删除用户全部角色绑定。
     */
    void removeUserRolesByUserId(String userId);

    Role getRoleById(String id);

    Permission getPermissionById(String id);
}
