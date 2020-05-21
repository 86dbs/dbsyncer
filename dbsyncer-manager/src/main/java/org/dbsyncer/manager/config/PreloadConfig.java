package org.dbsyncer.manager.config;

import org.dbsyncer.manager.enums.GroupStrategyEnum;
import org.dbsyncer.manager.enums.HandlerEnum;
import org.dbsyncer.manager.template.Handler;

public class PreloadConfig {

    private String filterType;

    private GroupStrategyEnum groupStrategyEnum;

    private HandlerEnum handlerEnum;

    public PreloadConfig(String filterType, HandlerEnum handlerEnum) {
        this.filterType = filterType;
        this.handlerEnum = handlerEnum;
    }

    public PreloadConfig(String filterType, GroupStrategyEnum groupStrategyEnum, HandlerEnum handlerEnum) {
        this.filterType = filterType;
        this.groupStrategyEnum = groupStrategyEnum;
        this.handlerEnum = handlerEnum;
    }

    public String getFilterType() {
        return filterType;
    }

    public GroupStrategyEnum getGroupStrategyEnum() {
        return groupStrategyEnum;
    }

    public HandlerEnum getHandlerEnum() {
        return handlerEnum;
    }
}