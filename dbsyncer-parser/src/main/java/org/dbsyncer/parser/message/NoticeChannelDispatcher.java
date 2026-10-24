/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.message;

import org.dbsyncer.common.model.RobotNoticeChannel;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.GroupRelProfile;
import org.dbsyncer.parser.NoticeRuleProfile;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.SystemConfigProfile;
import org.dbsyncer.parser.model.GroupRel;
import org.dbsyncer.parser.model.NoticeRule;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.NoticeChannelEnum;
import org.dbsyncer.sdk.model.NoticeConfig;
import org.dbsyncer.sdk.model.NoticeContent;
import org.dbsyncer.sdk.model.NoticeMessage;
import org.dbsyncer.sdk.notice.NoticeDeliveryHandler;
import org.dbsyncer.sdk.notice.NoticeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 将通知投递到全局已启用渠道；按分组/同步任务覆盖企微/钉钉 @ 与邮件收件人。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-16 12:00
 */
@Component
public class NoticeChannelDispatcher implements NoticeDeliveryHandler {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<NoticeChannelEnum, NoticeService> notifyServiceMap = new ConcurrentHashMap<>();

    @Resource
    private SystemConfigProfile systemConfigProfile;

    @Resource
    private GroupRelProfile groupRelProfile;

    @Resource
    private NoticeRuleProfile noticeRuleProfile;

    public void registerNotifyService(NoticeChannelEnum noticeChannelEnum, NoticeService notificationService) {
        notifyServiceMap.put(noticeChannelEnum, notificationService);
    }

    public void removeNotifyService(NoticeChannelEnum noticeChannelEnum) {
        notifyServiceMap.remove(noticeChannelEnum);
    }

    @Override
    public void deliver(NoticeContent noticeContent) {
        NoticeConfig noticeConfig = systemConfigProfile.getSystemConfig().getNoticeConfig();
        if (noticeConfig == null || noticeContent == null || notifyServiceMap.isEmpty()) {
            return;
        }
        NoticeRule rule = resolveNoticeRule(noticeContent);
        NoticeConfig deliveryConfig = copyWithAtOverride(noticeConfig, rule);
        NoticeMessage message = new NoticeMessage();
        message.setNoticeConfig(deliveryConfig);
        message.setNoticeContent(noticeContent);
        notifyServiceMap.forEach((channel, service) -> {
            try {
                setMessageReceiversIfMail(channel, message, rule);
                service.notify(message);
            } catch (Exception e) {
                logger.error("Send message error, channel: {}", channel, e);
                throw new ParserException(e);
            }
        });
    }

    /**
     * 解析规则：MAPPING 优先，否则合并所属 GROUP；无规则返回 {@code null}。
     */
    private NoticeConfig copyWithAtOverride(NoticeConfig source, NoticeRule rule) {
        if (rule == null) {
            return source;
        }
        NoticeConfig copy = JsonUtil.jsonToObj(JsonUtil.objToJson(source), NoticeConfig.class);
        if (copy == null) {
            return source;
        }
        applyAt(copy.getWechat(), rule);
        applyAt(copy.getDingTalk(), rule);
        return copy;
    }

    private NoticeRule resolveNoticeRule(NoticeContent content) {
        if (content == null) {
            return null;
        }
        if (!StringUtil.equals(ConfigConstant.MAPPING, content.getResourceType())
                || StringUtil.isBlank(content.getResourceId())) {
            return null;
        }
        String mappingId = content.getResourceId();
        NoticeRule mappingRule = noticeRuleProfile.getByScope(ConfigConstant.MAPPING, mappingId);
        if (mappingRule != null) {
            return mappingRule;
        }

        List<GroupRel> rels = groupRelProfile.listByResource(ConfigConstant.MAPPING, mappingId);
        if (CollectionUtils.isEmpty(rels)) {
            return null;
        }
        Set<String> groupIds = new LinkedHashSet<>();
        for (GroupRel rel : rels) {
            if (rel != null && StringUtil.isNotBlank(rel.getGroupId())) {
                groupIds.add(rel.getGroupId());
            }
        }
        if (groupIds.isEmpty()) {
            return null;
        }

        List<NoticeRule> groupRules = noticeRuleProfile.listByScopeType(ConfigConstant.GROUP);
        if (CollectionUtils.isEmpty(groupRules)) {
            return null;
        }

        boolean atAll = false;
        LinkedHashSet<String> phones = new LinkedHashSet<>();
        LinkedHashSet<String> emails = new LinkedHashSet<>();
        boolean matched = false;
        for (NoticeRule rule : groupRules) {
            if (rule == null || !groupIds.contains(rule.getScopeId())) {
                continue;
            }
            matched = true;
            if (rule.isAtAll()) {
                atAll = true;
            }
            if (StringUtil.isNotBlank(rule.getAtPhones())) {
                phones.addAll(Arrays.asList(StringUtil.split(rule.getAtPhones(), StringUtil.COMMA)));
            }
            if (StringUtil.isNotBlank(rule.getEmails())) {
                emails.addAll(Arrays.asList(StringUtil.split(rule.getEmails(), StringUtil.COMMA)));
            }
        }
        if (!matched) {
            return null;
        }
        NoticeRule merged = new NoticeRule();
        merged.setScopeType(ConfigConstant.GROUP);
        merged.setAtAll(atAll ? 1 : 0);
        merged.setAtPhones(StringUtil.join(phones, StringUtil.COMMA));
        merged.setEmails(StringUtil.join(emails, StringUtil.COMMA));
        return merged;
    }

    private void applyAt(RobotNoticeChannel channel, NoticeRule rule) {
        if (channel == null || rule == null) {
            return;
        }
        channel.setAtAll(rule.isAtAll());
        // Robot API 字段仍为 atMobiles，取值来自规则 atPhones
        channel.setAtMobiles(rule.getAtPhones());
    }

    private void setMessageReceiversIfMail(NoticeChannelEnum channel, NoticeMessage message, NoticeRule rule) {
        if (channel != NoticeChannelEnum.EMAIL) {
            return;
        }
        if (rule == null || StringUtil.isBlank(rule.getEmails())) {
            return;
        }
        message.setReceivers(new ArrayList<>(Arrays.asList(StringUtil.split(rule.getEmails(), StringUtil.COMMA))));
    }
}
