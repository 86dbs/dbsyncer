/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.notice;


import org.dbsyncer.sdk.model.NoticeMessage;

public interface NoticeService {

    /**
     * 发送通知消息
     */
    void notify(NoticeMessage notificationMessage);

}
