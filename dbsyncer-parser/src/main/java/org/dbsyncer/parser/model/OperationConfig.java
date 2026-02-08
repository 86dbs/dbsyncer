/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.enums.GroupStrategyEnum;

public class OperationConfig {

    private String id;

    private ConfigModel model;

    private GroupStrategyEnum groupStrategyEnum;

    private CommandEnum commandEnum;

    public OperationConfig(String id) {
        this(id, GroupStrategyEnum.DEFAULT);
    }

    public OperationConfig(String id, GroupStrategyEnum groupStrategyEnum) {
        this.id = id;
        this.groupStrategyEnum = groupStrategyEnum;
    }

    public OperationConfig(ConfigModel model, CommandEnum commandEnum) {
        this(model, commandEnum, GroupStrategyEnum.DEFAULT);
    }

    public OperationConfig(ConfigModel model, CommandEnum commandEnum, GroupStrategyEnum groupStrategyEnum) {
        this.model = model;
        this.commandEnum = commandEnum;
        this.groupStrategyEnum = groupStrategyEnum;
    }

    public String getId() {
        return id;
    }

    public ConfigModel getModel() {
        return model;
    }

    public GroupStrategyEnum getGroupStrategyEnum() {
        return groupStrategyEnum;
    }

    public CommandEnum getCommandEnum() {
        return commandEnum;
    }
}
