/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin.impl;

import com.alibaba.fastjson2.annotation.JSONField;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.plugin.AbstractNoticeService;
import org.dbsyncer.plugin.model.MappingErrorContent;
import org.dbsyncer.plugin.model.MappingStopContent;
import org.dbsyncer.plugin.model.NoticeContent;
import org.dbsyncer.plugin.model.TestNoticeContent;
import org.dbsyncer.plugin.model.WeChatNoticeChannel;
import org.dbsyncer.plugin.model.NoticeMessage;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 企业微信通知服务实现
 *
 * @author AE86
 * @version 1.0.0
 * @date 2026/03/05 22:14
 */
public final class WeChatNoticeService extends AbstractNoticeService {

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
            String atUserMobiles = wechat.getAtUserMobiles();
            if (StringUtil.isNotBlank(atUserMobiles)) {
                content.setMentionedMobileList(StringUtil.split(atUserMobiles, StringUtil.COMMA));
            }
        }
        params.setContent(content);
        send(wechat.getWebhookUrl(), JsonUtil.objToJson(params));
    }

    private String buildContent(NoticeContent noticeContent) {
        // 运行异常
        if (noticeContent instanceof MappingErrorContent) {
            MappingErrorContent meta = (MappingErrorContent) noticeContent;
            StringBuilder c = new StringBuilder();
            c.append(String.format("[%s] %s，任务数：%s\n",
                    getAppConfig().getName(), noticeContent.getTitle(), meta.getErrorItems().size()));
            c.append(String.format("时间:%s\n\n", DateFormatUtil.now()));

            for (int i = 0; i < meta.getErrorItems().size(); i++) {
                MappingErrorContent.ErrorItem item = meta.getErrorItems().get(i);
                c.append(String.format("%d. %s(%s) 失败:%s, 成功:%s", i+1, item.getName(), item.getModel().getName(), item.getFail(), item.getSuccess()));
                if (ModelEnum.FULL == item.getModel()) {
                    c.append(String.format("总数:%s", item.getTotal()));
                }
                c.append("\n");
            }
            return c.toString();
        }

        // 停止驱动
        if (noticeContent instanceof MappingStopContent) {
            MappingStopContent meta = (MappingStopContent) noticeContent;
            return String.format("[%s] %s %s(%s)", getAppConfig().getName(),
                    noticeContent.getTitle(), meta.getName(), meta.getModel().getName());
        }

        // 测试通知
        if (noticeContent instanceof TestNoticeContent) {
            TestNoticeContent meta = (TestNoticeContent) noticeContent;
            return String.format("[%s] %s %s", getAppConfig().getName(), noticeContent.getTitle(), meta.getContent());
        }
        return null;
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
