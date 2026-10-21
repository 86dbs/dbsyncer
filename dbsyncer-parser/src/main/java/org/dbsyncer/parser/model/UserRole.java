/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 用户-角色关系。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
public class UserRole extends ConfigModel {

    private String userId;
    private String roleId;

    public UserRole() {
        super.setName("");
        super.setType(ConfigConstant.USER_ROLE_REL);
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getRoleId() {
        return roleId;
    }

    public void setRoleId(String roleId) {
        this.roleId = roleId;
    }
}
