/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 业务分组关系。父类 {@link #getType()} 固定为 {@link ConfigConstant#GROUP_REL}（存储路由）；
 * 资源类型见 {@link #resourceType}。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-14 18:00
 */
public class GroupRel extends ConfigModel {

    /**
     * 分组 ID
     */
    private String groupId;

    /**
     * 资源类型：connector / mapping / validateSync / databaseSync
     */
    private String resourceType;

    /**
     * 资源 ID（连接或任务 ID）
     */
    private String resourceId;

    public GroupRel() {
        super.setName("");
        super.setType(ConfigConstant.GROUP_REL);
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
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
}
