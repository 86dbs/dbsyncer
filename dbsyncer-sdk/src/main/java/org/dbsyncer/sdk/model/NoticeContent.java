/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;


import org.dbsyncer.sdk.enums.NoticeTypeEnum;

/**
 * 通知消息
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2026-03-04 19:00
 */
public abstract class NoticeContent {

    private NoticeTypeEnum noticeType;

    private String title;

    /**
     * 资源类型（如 mapping），用于解析 @ 覆盖规则
     */
    private String resourceType;

    /**
     * 资源 ID（如同步任务 ID）
     */
    private String resourceId;

    public NoticeTypeEnum getNoticeType() {
        return noticeType;
    }

    public void setNoticeType(NoticeTypeEnum noticeType) {
        this.noticeType = noticeType;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
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
