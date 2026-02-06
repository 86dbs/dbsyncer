package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.UserInfo;

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

    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }
}