/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 资源访问控制。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
public class ResourceAcl extends ConfigModel {

    /**
     * 主体类型：0-USER / 1-ROLE
     */
    private int subjectType;

    private String subjectId;

    /**
     * group / connector / mapping / validateSync / databaseSync
     */
    private String resourceType;

    private String resourceId;

    /**
     * 0-只读 / 1-可读写
     */
    private int permission;

    public ResourceAcl() {
        super.setName("");
        super.setType(ConfigConstant.RESOURCE_ACL);
    }

    public int getSubjectType() {
        return subjectType;
    }

    public void setSubjectType(int subjectType) {
        this.subjectType = subjectType;
    }

    public String getSubjectId() {
        return subjectId;
    }

    public void setSubjectId(String subjectId) {
        this.subjectId = subjectId;
    }

    public String getResourceType() {
        return resourceType;
    }

    public void setResourceType(String resourceType) {
        this.resourceType = resourceType;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public int getPermission() {
        return permission;
    }

    public void setPermission(int permission) {
        this.permission = permission;
    }
}
