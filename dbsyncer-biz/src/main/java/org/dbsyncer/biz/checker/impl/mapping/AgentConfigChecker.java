/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.mapping;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.checker.MappingConfigChecker;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.sdk.config.ListenerConfig;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.model.CaptureRemoteTask;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Map;

/**
 * Agent 采集配置：只写入任务 JSON，不写关联表。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-13
 */
@Component
public class AgentConfigChecker implements MappingConfigChecker {

    @Resource
    private TaskProfile taskProfile;

    @Override
    public void modify(Mapping mapping, Map<String, String> params) {
        ListenerConfig listener = mapping.getListener();
        Assert.notNull(listener, "ListenerConfig can not be null.");
        String captureTaskId = StringUtil.trim(params.get("captureTaskId"));
        if (StringUtil.isBlank(captureTaskId)) {
            throw new BizException("请选择远端采集任务");
        }
        CaptureRemoteTask task = taskProfile.getTask(captureTaskId, CaptureRemoteTask.class);
        if (task == null || !ConfigConstant.CAPTURE_REMOTE.equals(task.getType())) {
            throw new BizException("只能选择远端采集任务");
        }
        listener.setListenerType(ListenerTypeEnum.AGENT.getType());
        listener.setCaptureTaskId(captureTaskId);
    }
}
