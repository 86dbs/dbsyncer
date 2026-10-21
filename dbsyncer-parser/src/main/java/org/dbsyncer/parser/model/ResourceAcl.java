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
     * USER / ROLE
     */
    private String subjectType;
    private String subjectId;
    /**
     * group / connector / mapping / validateSync / databaseSync
     */
    private String resourceType;
    private String resourceId;
    /**
     * 逗号分隔：read,write,operate
     */
    private String actions;

    public ResourceAcl() {
        super.setName("");
        super.setType(ConfigConstant.RESOURCE_ACL);
    }

    public String getSubjectType() {
        return subjectType;
    }

    public void setSubjectType(String subjectType) {
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

    public String getActions() {
        return actions;
    }

    public void setActions(String actions) {
        this.actions = actions;
    }
}
