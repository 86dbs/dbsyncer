/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin.model;


import org.dbsyncer.sdk.enums.NoticeTypeEnum;
import org.dbsyncer.sdk.model.NoticeContent;

/**
 * 通用通知消息
 *
 * @Author wuji
 * @Version 1.0.0
 * @Date 2026-06-08 00:00
 */
public class GeneralNoticeContent extends NoticeContent {

    private String content;

    public GeneralNoticeContent() {
        setNoticeType(NoticeTypeEnum.GENERAL_MESSAGE);
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

}
