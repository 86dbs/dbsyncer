/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.parser.enums.CommandEnum;

public class OperationConfig {

    private String id;

    private ConfigModel model;

    private CommandEnum commandEnum;

    public OperationConfig(String id) {
        this.id = id;
    }

    public OperationConfig(ConfigModel model, CommandEnum commandEnum) {
        this.model = model;
        this.commandEnum = commandEnum;
    }

    public String getId() {
        return id;
    }

    public ConfigModel getModel() {
        return model;
    }

    public CommandEnum getCommandEnum() {
        return commandEnum;
    }
}
