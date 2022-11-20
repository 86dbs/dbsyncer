package org.dbsyncer.parser.model;

import org.dbsyncer.storage.constant.ConfigConstant;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/16 23:16
 */
public class ConfigModel {

    private String id;

    /**
     * system/user/connector/mapping/tableGroup/meta/projectGroup
     *
     * @see ConfigConstant
     */
    private String type;

    private String name;

    private Long createTime;

    private Long updateTime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public ConfigModel setType(String type) {
        this.type = type;
        return this;
    }

    public String getName() {
        return name;
    }

    public ConfigModel setName(String name) {
        this.name = name;
        return this;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public ConfigModel setCreateTime(Long createTime) {
        this.createTime = createTime;
        return this;
    }

    public Long getUpdateTime() {
        return updateTime;
    }

    public ConfigModel setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
        return this;
    }
}