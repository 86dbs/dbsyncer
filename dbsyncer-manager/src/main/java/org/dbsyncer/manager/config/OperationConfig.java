package org.dbsyncer.manager.config;

import org.dbsyncer.manager.enums.GroupStrategyEnum;
import org.dbsyncer.manager.enums.HandlerEnum;
import org.dbsyncer.manager.template.Handler;
import org.dbsyncer.parser.model.ConfigModel;

public class OperationConfig {

    private String id;

    private ConfigModel model;

    private GroupStrategyEnum groupStrategyEnum = GroupStrategyEnum.DEFAULT;

    private HandlerEnum handlerEnum;

    public OperationConfig(String id) {
        this.id = id;
    }

    public OperationConfig(String id, GroupStrategyEnum groupStrategyEnum) {
        this.id = id;
        this.groupStrategyEnum = groupStrategyEnum;
    }

    public OperationConfig(ConfigModel model, HandlerEnum handlerEnum) {
        this.model = model;
        this.handlerEnum = handlerEnum;
    }

    public OperationConfig(ConfigModel model, HandlerEnum handlerEnum, GroupStrategyEnum groupStrategyEnum) {
        this.model = model;
        this.handlerEnum = handlerEnum;
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

    public Handler getHandler() {
        return handlerEnum.getHandler();
    }
}