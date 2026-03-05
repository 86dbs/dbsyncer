/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin.impl;

import com.alibaba.fastjson2.annotation.JSONField;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.plugin.AbstractNoticeService;
import org.dbsyncer.plugin.model.WeChatNoticeChannel;
import org.dbsyncer.plugin.model.NoticeMessage;

/**
 * 企业微信通知服务实现
 *
 * @author AE86
 * @version 1.0.0
 * @date 2026/03/05 22:14
 */
public final class WeChatNoticeService extends AbstractNoticeService {

    @Override
    public void notify(NoticeMessage notificationMessage) {
        WeChatNoticeChannel wechat = notificationMessage.getNoticeConfig().getWechat();
        if (!wechat.isEnabled()) {
            return;
        }
        String message = notificationMessage.getContent();
        MessageContent params = new MessageContent();
        params.setMsgType("text");
        TextContent text = new TextContent();
        text.setContent(message);

        // @所有人
        if (wechat.isAtAll()) {
            text.setMentionedList(StringUtil.split("@all", StringUtil.COMMA));
        } else {
            // @某个人手机号
            String atUserMobiles = wechat.getAtUserMobiles();
            if (StringUtil.isNotBlank(atUserMobiles)) {
                text.setMentionedMobileList(StringUtil.split(atUserMobiles, StringUtil.COMMA));
            }
        }
        params.setText(text);
        send(wechat.getWebhookUrl(), JsonUtil.objToJson(params));
    }

    static final class MessageContent {
        @JSONField(name = "msgtype")
        private String msgType;

        @JSONField(name = "text")
        private Object text;

        public String getMsgType() {
            return msgType;
        }

        public void setMsgType(String msgType) {
            this.msgType = msgType;
        }

        public Object getText() {
            return text;
        }

        public void setText(Object text) {
            this.text = text;
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
