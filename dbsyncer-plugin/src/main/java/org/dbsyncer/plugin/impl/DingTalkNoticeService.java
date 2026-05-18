/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin.impl;

import com.alibaba.fastjson2.annotation.JSONField;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.model.DingTalkNoticeChannel;
import org.dbsyncer.sdk.model.NoticeContent;
import org.dbsyncer.sdk.model.NoticeMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 钉钉通知服务实现
 *
 * @author AE86
 * @version 1.0.0
 * @date 2026/03/05 22:14
 */
public final class DingTalkNoticeService extends AbstractWebHookNoticeService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void notify(NoticeMessage noticeMessage) {
        DingTalkNoticeChannel dingTalk = noticeMessage.getNoticeConfig().getDingTalk();
        if (!dingTalk.isEnabled()) {
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
        At at = new At();
        // @所有人
        if (dingTalk.isAtAll()) {
            at.setAtAll(true);
        } else {
            // @某个人手机号
            String atMobiles = dingTalk.getAtMobiles();
            if (StringUtil.isNotBlank(atMobiles)) {
                at.setAtMobiles(StringUtil.split(atMobiles, StringUtil.COMMA));
            }
        }
        params.setAt(at);
        TextContent content = new TextContent();
        content.setContent(c);
        params.setContent(content);
        send(dingTalk.getWebhookUrl(), JsonUtil.objToJson(params));
    }

    static final class MessageContent {
        @JSONField(name = "msgtype")
        private String msgType;

        @JSONField(name = "text")
        private Object content;

        /**
         * @用户配置
         */
        private At at;

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

        public At getAt() {
            return at;
        }

        public void setAt(At at) {
            this.at = at;
        }
    }

    static final class At {

        /**
         * At手机号列表
         */
        @JSONField(name = "atMobiles")
        private String[] atMobiles;

        /**
         * At所有人
         */
        @JSONField(name = "isAtAll")
        private boolean isAtAll;

        public String[] getAtMobiles() {
            return atMobiles;
        }

        public void setAtMobiles(String[] atMobiles) {
            this.atMobiles = atMobiles;
        }

        public boolean isAtAll() {
            return isAtAll;
        }

        public void setAtAll(boolean atAll) {
            isAtAll = atAll;
        }
    }

    static final class TextContent {

        @JSONField(name = "content")
        private String content;

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }

    }

}
