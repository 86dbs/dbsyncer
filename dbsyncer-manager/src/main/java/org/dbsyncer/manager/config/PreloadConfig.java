package org.dbsyncer.manager.config;

import org.dbsyncer.manager.enums.GroupStrategyEnum;
import org.dbsyncer.manager.enums.HandlerEnum;

public class PreloadConfig {

    private String configModelType;

    private GroupStrategyEnum groupStrategyEnum;

    private HandlerEnum handlerEnum;

    public PreloadConfig(String configModelType, HandlerEnum handlerEnum) {
        this.configModelType = configModelType;
        this.handlerEnum = handlerEnum;
    }

    public PreloadConfig(String configModelType, GroupStrategyEnum groupStrategyEnum, HandlerEnum handlerEnum) {
        this.configModelType = configModelType;
        this.groupStrategyEnum = groupStrategyEnum;
        this.handlerEnum = handlerEnum;
    }

    public String getConfigModelType() {
        return configModelType;
    }

    public GroupStrategyEnum getGroupStrategyEnum() {
        return groupStrategyEnum;
    }

    public HandlerEnum getHandlerEnum() {
        return handlerEnum;
    }
}