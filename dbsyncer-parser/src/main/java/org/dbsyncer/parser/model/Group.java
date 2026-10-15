/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 业务分组（{@code dbsyncer_group}）。
 * <p>勿与表映射 {@link TableGroup} 混淆。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-14 18:00
 */
public class Group extends ConfigModel {

    /**
     * 排序，越小越靠前
     */
    private int sort;

    /**
     * 归属用户 ID，空表示未指定
     */
    private String ownerId = "";

    /**
     * 归属角色，空表示未指定
     */
    private String ownerRole = "";

    public Group() {
        super.setType(ConfigConstant.GROUP);
    }

    public int getSort() {
        return sort;
    }

    public void setSort(int sort) {
        this.sort = sort;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getOwnerRole() {
        return ownerRole;
    }

    public void setOwnerRole(String ownerRole) {
        this.ownerRole = ownerRole;
    }
}
