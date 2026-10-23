/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 权限点。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
public class Permission extends ConfigModel {

    private String code;
    private String module;
    /**
     * 种类、类别 MENU / API / ACTION
     */
    private String kind;
    private String remark = "";

    public Permission() {
        super.setType(ConfigConstant.PERMISSION);
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getModule() {
        return module;
    }

    public void setModule(String module) {
        this.module = module;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }
}
