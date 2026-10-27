/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.message;

import org.dbsyncer.parser.TaskMetaProfile;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.plugin.model.MappingFullCompleteContent;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.notice.MessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 纯全量自然完成后发送通知（单机 / 分布式共用）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-27
 */
@Component
public class NoticeMappingCompletedPublisher {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private MessageService messageService;

    @Resource
    private TaskMetaProfile taskMetaProfile;

    /**
     * 发送全量同步完成通知；失败仅记日志，不影响主流程。
     */
    public void publish(Mapping mapping) {
        if (mapping == null) {
            return;
        }
        try {
            MappingFullCompleteContent content = new MappingFullCompleteContent();
            content.setTitle("全量同步完成");
            content.setName(mapping.getName());
            content.setModel(ModelEnum.getModelEnum(mapping.getModel()));
            content.setResourceType(ConfigConstant.MAPPING);
            content.setResourceId(mapping.getId());
            Meta meta = taskMetaProfile.getMeta(mapping.getId());
            if (meta != null) {
                content.setSuccess(meta.getSuccess().get());
                content.setFail(meta.getFail().get());
                content.setTotal(meta.getTotal().get());
            }
            messageService.sendMessage(content);
        } catch (Exception e) {
            logger.error("发送全量完成通知失败: {}", e.getMessage(), e);
        }
    }
}
