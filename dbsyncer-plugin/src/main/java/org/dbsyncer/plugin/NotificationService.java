/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin;

import org.dbsyncer.plugin.model.NoticeMessage;

public interface NotificationService {

    /**
     * 发送通知消息
     */
    void notify(NoticeMessage notificationMessage);

}
