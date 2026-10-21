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
     * 角色名称
     */
    private String roleName;

    /**
     * 是否管理员角色
     */
    private boolean admin;

    /**
     * 功能权限码列表（如 mapping:operate）
     */
    private List<String> permissions = new ArrayList<>();

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
}
