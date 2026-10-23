/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.message;

import org.dbsyncer.common.model.RobotNoticeChannel;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.GroupRelProfile;
import org.dbsyncer.parser.NoticeMentionRuleProfile;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.SystemConfigProfile;
import org.dbsyncer.parser.UserProfile;
import org.dbsyncer.parser.model.GroupRel;
import org.dbsyncer.parser.model.NoticeMentionRule;
import org.dbsyncer.parser.model.UserConfig;
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
 * 将通知投递到全局已启用渠道；按分组/同步任务覆盖微信/钉钉 @。
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
    private UserProfile userProfile;

    @Resource
    private GroupRelProfile groupRelProfile;

    @Resource
    private NoticeMentionRuleProfile noticeMentionRuleProfile;

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
        NoticeConfig deliveryConfig = copyWithMentionOverride(noticeConfig, noticeContent);
        NoticeMessage message = new NoticeMessage();
        message.setNoticeConfig(deliveryConfig);
        message.setNoticeContent(noticeContent);
        notifyServiceMap.forEach((channel, service) -> {
            try {
                setMessageReceiversIfMail(channel, message);
                service.notify(message);
            } catch (Exception e) {
                logger.error("Send message error, channel: {}", channel, e);
                throw new ParserException(e);
            }
        });
    }

    /**
     * 解析 @：MAPPING 规则优先，否则合并所属 GROUP 规则；无规则则不 @。
     */
    private NoticeConfig copyWithMentionOverride(NoticeConfig source, NoticeContent content) {
        NoticeMentionRule override = resolveMentionRule(content);
        if (override == null) {
            return source;
        }
        NoticeConfig copy = JsonUtil.jsonToObj(JsonUtil.objToJson(source), NoticeConfig.class);
        if (copy == null) {
            return source;
        }
        applyAt(copy.getWechat(), override);
        applyAt(copy.getDingTalk(), override);
        return copy;
    }

    private NoticeMentionRule resolveMentionRule(NoticeContent content) {
        if (content == null) {
            return null;
        }
        if (!StringUtil.equals(ConfigConstant.MAPPING, content.getResourceType())
                || StringUtil.isBlank(content.getResourceId())) {
            return null;
        }
        String mappingId = content.getResourceId();
        NoticeMentionRule mappingRule = noticeMentionRuleProfile.getByScope(ConfigConstant.MAPPING, mappingId);
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

        List<NoticeMentionRule> groupRules = noticeMentionRuleProfile.listByScopeType(ConfigConstant.GROUP);
        if (CollectionUtils.isEmpty(groupRules)) {
            return null;
        }

        boolean atAll = false;
        LinkedHashSet<String> mobiles = new LinkedHashSet<>();
        boolean matched = false;
        for (NoticeMentionRule rule : groupRules) {
            if (rule == null || !groupIds.contains(rule.getScopeId())) {
                continue;
            }
            matched = true;
            if (rule.isAtAll()) {
                atAll = true;
            }
            if (StringUtil.isNotBlank(rule.getAtMobiles())) {
                mobiles.addAll(Arrays.asList(StringUtil.split(rule.getAtMobiles(), StringUtil.COMMA)));
            }
        }
        if (!matched) {
            return null;
        }
        NoticeMentionRule merged = new NoticeMentionRule();
        merged.setScopeType(ConfigConstant.GROUP);
        merged.setAtAll(atAll ? 1 : 0);
        merged.setAtMobiles(StringUtil.join(mobiles, StringUtil.COMMA));
        return merged;
    }

    private void applyAt(RobotNoticeChannel channel, NoticeMentionRule rule) {
        if (channel == null || rule == null) {
            return;
        }
        channel.setAtAll(rule.isAtAll());
        channel.setAtMobiles(rule.getAtMobiles());
    }

    private void setMessageReceiversIfMail(NoticeChannelEnum channel, NoticeMessage message) {
        if (channel != NoticeChannelEnum.EMAIL) {
            return;
        }
        UserConfig userConfig = userProfile.getUserConfig();
        if (userConfig == null || CollectionUtils.isEmpty(userConfig.getUserInfoList())) {
            return;
        }
        List<String> receivers = new ArrayList<>();
        userConfig.getUserInfoList().forEach(user -> {
            if (user != null && StringUtil.isNotBlank(user.getEmail())) {
                receivers.add(user.getEmail());
            }
        });
        message.setReceivers(receivers);
    }
}
