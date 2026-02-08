/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.MessageService;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.enums.ModelEnum;

import org.springframework.util.Assert;

import javax.annotation.Resource;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BaseServiceImpl {

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private LogService logService;

    @Resource
    private MessageService messageService;

    /**
     * 驱动启停锁
     */
    protected final static Object LOCK = new Object();

    protected boolean isRunning(String metaId) {
        Meta meta = profileComponent.getMeta(metaId);
        if (null != meta) {
            int state = meta.getState();
            return MetaEnum.isRunning(state);
        }
        return false;
    }

    protected void assertRunning(String metaId) {
        synchronized (LOCK) {
            Assert.isTrue(!isRunning(metaId), "驱动正在运行, 请先停止.");
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
            // 新增驱动:知识库(全量)
            String model = ModelEnum.getModelEnum(mapping.getModel()).getName();
            logService.log(log, "%s%s:%s(%s)", log.getMessage(), log.getName(), mapping.getName(), model);
        }
    }

    protected void log(LogType log, TableGroup tableGroup) {
        if (null != tableGroup) {
            Mapping mapping = profileComponent.getMapping(tableGroup.getMappingId());
            if (null != mapping) {
                // 新增驱动知识库(全量)映射关系:[My_User] >> [My_User_Target]
                String name = mapping.getName();
                String model = ModelEnum.getModelEnum(mapping.getModel()).getName();
                String s = tableGroup.getSourceTable().getName();
                String t = tableGroup.getTargetTable().getName();
                logService.log(log, "%s驱动%s(%s)%s:[%s] >> [%s]", log.getMessage(), name, model, log.getName(), s, t);
            }
        }
    }

    /**
     * 发送通知消息
     */
    protected void sendNotifyMessage(String title, String content) {
        messageService.sendMessage(title, content);
    }

    protected <T> Paging<T> searchConfigModel(Map<String, String> params, List<T> list) {
        String searchKey = params.get("searchKey");
        if (StringUtil.isNotBlank(searchKey)) {
            list = list.stream().filter(c-> {
                if (c instanceof Connector || c instanceof Mapping) {
                    ConfigModel m = (ConfigModel) c;
                    return StringUtil.contains(m.getName(), searchKey);
                }
                if (c instanceof TableGroup) {
                    TableGroup tg = (TableGroup) c;
                    return StringUtil.contains(tg.getSourceTable().getName(), searchKey) || StringUtil.contains(tg.getTargetTable().getName(), searchKey);
                }
                return false;
            }).collect(Collectors.toList());
        }
        int pageNum = NumberUtil.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtil.toInt(params.get("pageSize"), 10);
        Paging<T> paging = new Paging<>(pageNum, pageSize);
        if (!CollectionUtils.isEmpty(list)) {
            paging.setTotal(list.size());
            int offset = (pageNum * pageSize) - pageSize;
            paging.setData(list.stream().skip(offset).limit(pageSize).collect(Collectors.toList()));
        }
        return paging;
    }
}