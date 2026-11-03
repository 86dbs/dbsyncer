/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.PermissionVO;
import org.dbsyncer.biz.vo.RoleVO;

import java.util.List;
import java.util.Map;

/**
 * 角色管理（列表、自建、绑权、删除）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
public interface RoleService {

    /**
     * 角色列表（含权限数量）。
     */
    List<RoleVO> listRoles();

    /**
     * 角色详情（含已绑权限码）。
     */
    RoleVO getRole(String roleId);

    /**
     * 全部权限点（可按角色标记 granted）。
     *
     * @param roleId 可空；非空时标记该角色是否已授权
     */
    List<PermissionVO> listPermissions(String roleId);

    /**
     * 新增角色并绑定权限码。
     *
     * @param params name、permissionCodes（逗号分隔）
     */
    String add(Map<String, String> params);

    /**
     * 更新角色名称，并覆盖绑定权限码。
     *
     * @param params roleId、name、permissionCodes（逗号分隔）
     */
    String edit(Map<String, String> params);

    /**
     * 删除角色（仍有用户绑定时不可删）。
     *
     * @param params roleId
     */
    String remove(Map<String, String> params);
}
