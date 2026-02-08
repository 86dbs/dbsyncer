/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * 任务配置表实体类
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-10-18 21:18
 */
public class CommonTask implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 唯一ID
     */
    private String id;

    /**
     * 创建时间
     */
    private Timestamp createTime;

    /**
     * 更新时间
     */
    private Timestamp updateTime;

    /**
     * 任务名称
     */
    private String name;

    /**
     * 任务状态, 0-未执行；1-执行中；2-执行成功；3-执行失败；
     * {@link CommonTaskStatusEnum}
     */
    private Integer status;

    /**
     * 任务类型
     */
    private String type;

    /**
     * 配置信息
     */
    private String json;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    public Timestamp getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Timestamp updateTime) {
        this.updateTime = updateTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }
}
