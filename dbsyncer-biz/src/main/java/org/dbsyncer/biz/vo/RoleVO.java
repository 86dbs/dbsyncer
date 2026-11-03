/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.Role;

import java.util.ArrayList;
import java.util.List;

/**
 * 角色展示（含已绑权限码）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
public class RoleVO extends Role {

    /**
     * 已绑定权限数量
     */
    private int permissionCount;

    /**
     * 已绑定权限码
     */
    private List<String> permissionCodes = new ArrayList<>();

    public int getPermissionCount() {
        return permissionCount;
    }

    public void setPermissionCount(int permissionCount) {
        this.permissionCount = permissionCount;
    }

    public List<String> getPermissionCodes() {
        return permissionCodes;
    }

    public void setPermissionCodes(List<String> permissionCodes) {
        this.permissionCodes = permissionCodes;
    }
}
