/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.ConfigModel;

/**
 * 业务分组关系。{@link #getType()} 复用父类字段，存资源类型（connector/mapping/…）。
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
     * 资源 ID（连接或任务 ID）
     */
    private String resourceId;

    public GroupRel() {
        super.setName("");
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }
}
