/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;

import java.io.Serializable;

/**
 * 通用任务配置
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2025-10-18 21:18
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
    private Long createTime;

    /**
     * 更新时间
     */
    private Long updateTime;

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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public Long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Long updateTime) {
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

}
