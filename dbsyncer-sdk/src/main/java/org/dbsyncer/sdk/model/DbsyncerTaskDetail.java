package org.dbsyncer.sdk.model;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * 任务明细表实体类
 * 对应表：dbsyncer_task_detail
 */
public class DbsyncerTaskDetail implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 唯一ID
     */
    private String id;

    /**
     * 关联的任务id
     */
    private String taskId;

    /**
     * 任务类型, dataVerification
     */
    private String type;

    /**
     * 数据源表名称
     */
    private String sourceTableName;

    /**
     * 目标源表名称
     */
    private String targetTableName;

    /**
     * 执行结果(上限8192个字符)
     */
    private String content;

    /**
     * 创建时间
     */
    private Timestamp createTime;

    /**
     * 更新时间
     */
    private Timestamp updateTime;

    // 构造方法
    public DbsyncerTaskDetail() {
    }

    // getter和setter方法
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
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

}