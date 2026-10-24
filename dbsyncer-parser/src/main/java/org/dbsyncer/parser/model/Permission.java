/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 菜单权限。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
public class Permission extends ConfigModel {

    private String code;
    private String module;

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
}
