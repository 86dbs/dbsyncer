/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.Permission;

/**
 * 权限点展示。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
public class PermissionVO extends Permission {

    /**
     * 模块展示名
     */
    private String moduleName;

    /**
     * 是否已授予当前编辑角色
     */
    private boolean granted;

    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    public boolean isGranted() {
        return granted;
    }

    public void setGranted(boolean granted) {
        this.granted = granted;
    }
}
