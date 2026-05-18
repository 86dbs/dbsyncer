/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin.impl;

import com.alibaba.fastjson2.annotation.JSONField;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;

import org.dbsyncer.sdk.model.NoticeContent;
import org.dbsyncer.sdk.model.NoticeMessage;
import org.dbsyncer.common.model.WeChatNoticeChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 企业微信通知服务实现
 *
 * @author AE86
 * @version 1.0.0
 * @date 2026/03/05 22:14
 */
public final class WeChatNoticeService extends AbstractWebHookNoticeService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void notify(NoticeMessage noticeMessage) {
        WeChatNoticeChannel wechat = noticeMessage.getNoticeConfig().getWechat();
        if (!wechat.isEnabled()) {
            return;
        }
        NoticeContent noticeContent = noticeMessage.getNoticeContent();
        String c = buildContent(noticeContent);
        if (StringUtil.isBlank(c)) {
            logger.warn("build content is blank, skip send, notice content:{}", noticeContent.getClass().getName());
            return;
        }
        MessageContent params = new MessageContent();
        params.setMsgType("text");
        TextContent content = new TextContent();
        content.setContent(c);

        // @所有人
        if (wechat.isAtAll()) {
            content.setMentionedList(StringUtil.split("@all", StringUtil.COMMA));
        } else {
            // @某个人手机号
            String atMobiles = wechat.getAtMobiles();
            if (StringUtil.isNotBlank(atMobiles)) {
                content.setMentionedMobileList(StringUtil.split(atMobiles, StringUtil.COMMA));
            }
        }
        params.setContent(content);
        send(wechat.getWebhookUrl(), JsonUtil.objToJson(params));
    }

    static final class MessageContent {
        @JSONField(name = "msgtype")
        private String msgType;

        @JSONField(name = "text")
        private Object content;

        public String getMsgType() {
            return msgType;
        }

        public void setMsgType(String msgType) {
            this.msgType = msgType;
        }

        public Object getContent() {
            return content;
        }

        public void setContent(Object content) {
            this.content = content;
        }
    }

    static final class TextContent {

        @JSONField(name = "content")
        private String content;

        /**
         * At用户列表
         */
        @JSONField(name = "mentioned_list")
        private String[] mentionedList;

        /**
         * At手机号列表
         */
        @JSONField(name = "mentioned_mobile_list")
        private String[] mentionedMobileList;

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }

        public String[] getMentionedList() {
            return mentionedList;
        }

        public void setMentionedList(String[] mentionedList) {
            this.mentionedList = mentionedList;
        }

        public String[] getMentionedMobileList() {
            return mentionedMobileList;
        }

        public void setMentionedMobileList(String[] mentionedMobileList) {
            this.mentionedMobileList = mentionedMobileList;
        }
    }

}
