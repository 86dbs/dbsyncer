/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.model.NoticeContent;
import org.dbsyncer.sdk.notice.MessageService;
import org.springframework.util.Assert;

import javax.annotation.Resource;

public class BaseServiceImpl {

    @Resource
    private TaskProfile taskProfile;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private LogService logService;

    @Resource
    private MessageService messageService;

    /**
     * 驱动启停锁
     */
    protected final static Object LOCK = new Object();

    protected boolean isRunning(String metaId) {
        Meta meta = metaProfile.getMeta(metaId);
        if (null != meta) {
            int state = meta.getState();
            return CommonTaskStatusEnum.isRunning(state);
        }
        return false;
    }

    protected void assertRunning(String metaId) {
        synchronized (LOCK) {
            Assert.isTrue(!isRunning(metaId), "任务正在运行, 请先停止.");
        }
    }

    protected void assertRunning(Mapping mapping) {
        Assert.notNull(mapping, "mapping can not be null.");
        assertRunning(mapping.getMetaId());
    }

    protected void log(LogType log, ConfigModel model) {
        if (null != model) {
            // 新增连接器:知识库
            logService.log(log, "%s%s:%s", log.getMessage(), log.getName(), model.getName());
        }
    }

    protected void log(LogType log, Mapping mapping) {
        if (null != mapping) {
            // 新增同步任务:知识库(全量)
            String model = ModelEnum.getModelEnum(mapping.getModel()).getName();
            logService.log(log, "%s%s:%s(%s)", log.getMessage(), log.getName(), mapping.getName(), model);
        }
    }

    protected void log(LogType log, TableGroup tableGroup) {
        if (null != tableGroup) {
            Mapping mapping = taskProfile.getMapping(tableGroup.getTaskId());
            if (null != mapping) {
                // 新增同步任务知识库(全量)映射关系:[My_User] >> [My_User_Target]
                String name = mapping.getName();
                String model = ModelEnum.getModelEnum(mapping.getModel()).getName();
                String s = tableGroup.getSourceTable().getName();
                String t = tableGroup.getTargetTable().getName();
                logService.log(log, "%s同步任务%s(%s)%s:[%s] >> [%s]", log.getMessage(), name, model, log.getName(), s, t);
            }
        }
    }

    /**
     * 发送通知消息
     */
    protected void sendNotifyMessage(NoticeContent content) {
        messageService.sendMessage(content);
    }

}