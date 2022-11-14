package org.dbsyncer.manager.enums;

import org.dbsyncer.manager.CommandExecutor;
import org.dbsyncer.manager.command.PersistenceCommand;
import org.dbsyncer.manager.command.PreloadCommand;
import org.dbsyncer.storage.constant.ConfigConstant;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/24 14:19
 */
public enum CommandEnum {

    /**
     * 添加
     */
    OPR_ADD("add", (cmd) -> ((PersistenceCommand) cmd).add()),

    /**
     * 修改
     */
    OPR_EDIT("edit", (cmd) -> ((PersistenceCommand) cmd).edit()),

    /**
     * 预加载Connector
     */
    PRELOAD_CONNECTOR(ConfigConstant.CONNECTOR, true, (cmd) -> ((PreloadCommand) cmd).parseConnector()),

    /**
     * 预加载Mapping
     */
    PRELOAD_MAPPING(ConfigConstant.MAPPING, true, (cmd) -> ((PreloadCommand) cmd).parseMapping()),

    /**
     * 预加载TableGroup
     */
    PRELOAD_TABLE_GROUP(ConfigConstant.TABLE_GROUP, true, GroupStrategyEnum.TABLE, (cmd) -> ((PreloadCommand) cmd).parseTableGroup()),

    /**
     * 预加载Meta
     */
    PRELOAD_META(ConfigConstant.META, true, (cmd) -> ((PreloadCommand) cmd).parseMeta()),

    /**
     * 预加载Config
     */
    PRELOAD_CONFIG(ConfigConstant.CONFIG, true, (cmd) -> ((PreloadCommand) cmd).parseConfig()),

    /**
     * 预加载ProjectGroup
     */
    PRELOAD_PROJECT_GROUP(ConfigConstant.PROJECT_GROUP, true, (cmd) -> ((PreloadCommand) cmd).parseProjectGroup());

    private String modelType;
    private boolean preload;
    private CommandExecutor commandExecutor;
    private GroupStrategyEnum groupStrategyEnum;

    CommandEnum(String modelType, CommandExecutor commandExecutor) {
        this(modelType, false, commandExecutor);
    }

    CommandEnum(String modelType, boolean preload, CommandExecutor commandExecutor) {
        this(modelType, preload, GroupStrategyEnum.DEFAULT, commandExecutor);
    }

    CommandEnum(String modelType, boolean preload, GroupStrategyEnum groupStrategyEnum, CommandExecutor commandExecutor) {
        this.modelType = modelType;
        this.preload = preload;
        this.groupStrategyEnum = groupStrategyEnum;
        this.commandExecutor = commandExecutor;
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