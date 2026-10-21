/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 角色。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
public class Role extends ConfigModel {

    private String code;
    private int builtin;
    private String remark = "";

    public Role() {
        super.setType(ConfigConstant.ROLE);
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public int getBuiltin() {
        return builtin;
    }

    public void setBuiltin(int builtin) {
        this.builtin = builtin;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }
}
