package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.UserInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/17 0:14
 */
public class UserInfoVO extends UserInfo {

    /**
     * 主角色 ID（dbsyncer_user_role）
     */
    private String roleId;

    /**
     * 角色名称
     */
    private String roleName;

    /**
     * 是否超管（username=admin）
     */
    private boolean admin;

    /**
     * 功能权限码列表（如 mapping:operate）
     */
    private List<String> permissions = new ArrayList<>();

    /**
     * 是否因登录失败被临时锁定（进程内状态，不持久化）
     */
    private boolean locked;

    public String getRoleId() {
        return roleId;
    }

    public void setRoleId(String roleId) {
        this.roleId = roleId;
    }

    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    public boolean isAdmin() {
        return admin;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public List<String> getPermissions() {
        return permissions;
    }

    public void setPermissions(List<String> permissions) {
        this.permissions = permissions;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }
}
