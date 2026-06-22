/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.notice;

import org.dbsyncer.sdk.model.NoticeContent;

/**
 * 将通知投递到已启用的全局渠道。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-16 12:00
 */
public interface NoticeDeliveryHandler {

    /**
     * 投递通知到全局已启用渠道。
     *
     * @param noticeContent 通知内容
     */
    void deliver(NoticeContent noticeContent);
}
