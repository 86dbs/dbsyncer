/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin.model;

import org.dbsyncer.plugin.enums.NoticeTypeEnum;

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
}
