/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin.impl;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.plugin.AbstractNoticeService;
import org.dbsyncer.plugin.model.HttpNoticeChannel;
import org.dbsyncer.plugin.model.NoticeContent;
import org.dbsyncer.plugin.model.NoticeMessage;

/**
 * HTTP通知服务实现
 *
 * @author AE86
 * @version 1.0.0
 * @date 2026/03/05 22:14
 */
public final class HttpNoticeService extends AbstractNoticeService {

    @Override
    public void notify(NoticeMessage noticeMessage) {
        HttpNoticeChannel config = noticeMessage.getNoticeConfig().getHttp();
        if (!config.isEnabled()) {
            return;
        }
        NoticeContent noticeContent = noticeMessage.getNoticeContent();
        send(config.getUrl(), JsonUtil.objToJson(noticeContent));
    }

}