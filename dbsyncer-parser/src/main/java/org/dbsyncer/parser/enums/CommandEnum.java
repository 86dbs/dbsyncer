/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.enums;

import org.dbsyncer.parser.command.CommandExecutor;
import org.dbsyncer.parser.command.Persistence;
import org.dbsyncer.parser.command.Preload;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 枚举命令模式: 持久化和预加载
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/24 14:19
 */
public enum CommandEnum {

    /**
     * 添加
     */
    OPR_ADD("add", Persistence::addConfig),

    /**
     * 修改
     */
    OPR_EDIT("edit", Persistence::editConfig),

    /**
     * 预加载SystemConfig
     */
    PRELOAD_SYSTEM(ConfigConstant.SYSTEM, Preload::parseSystemConfig, true),

    /**
     * 预加载UserConfig
     */
    PRELOAD_USER(ConfigConstant.USER, Preload::parseUserConfig, true),

    /**
     * 预加载Connector
     */
    PRELOAD_CONNECTOR(ConfigConstant.CONNECTOR, Preload::parseConnector, true),

    /**
     * 预加载Mapping
     */
    PRELOAD_MAPPING(ConfigConstant.MAPPING, Preload::parseMapping, true),

    /**
     * 预加载TableGroup
     */
    PRELOAD_TABLE_GROUP(ConfigConstant.TABLE_GROUP, Preload::parseTableGroup, true, GroupStrategyEnum.TABLE),

    /**
     * 预加载Meta
     */
    PRELOAD_META(ConfigConstant.META, Preload::parseMeta, true);

    /**
     * 命令类型
     */
    private final String modelType;

    /**
     * 执行器
     */
    private final CommandExecutor commandExecutor;

    /**
     * 是否预加载
     */
    private final boolean preload;

    /**
     * 分组持久化策略
     */
    private final GroupStrategyEnum groupStrategyEnum;

    CommandEnum(String modelType, CommandExecutor commandExecutor) {
        this(modelType, commandExecutor, false);
    }

    CommandEnum(String modelType, CommandExecutor commandExecutor, boolean preload) {
        this(modelType, commandExecutor, preload, GroupStrategyEnum.DEFAULT);
    }

    CommandEnum(String modelType, CommandExecutor commandExecutor, boolean preload, GroupStrategyEnum groupStrategyEnum) {
        this.modelType = modelType;
        this.commandExecutor = commandExecutor;
        this.preload = preload;
        this.groupStrategyEnum = groupStrategyEnum;
    }

    public String getModelType() {
        return modelType;
    }

    public boolean isPreload() {
        return preload;
    }

    public CommandExecutor getCommandExecutor() {
        return commandExecutor;
    }

    public GroupStrategyEnum getGroupStrategyEnum() {
        return groupStrategyEnum;
    }
}
