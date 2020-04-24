package org.dbsyncer.manager.config;

import org.dbsyncer.manager.enums.GroupStrategyEnum;
import org.dbsyncer.manager.template.Handler;

public class PreloadConfig {

    private String filterType;

    private GroupStrategyEnum groupStrategyEnum;

    private Handler handler;

    public PreloadConfig(String filterType, Handler handler) {
        this.filterType = filterType;
        this.handler = handler;
    }

    public PreloadConfig(String filterType, GroupStrategyEnum groupStrategyEnum, Handler handler) {
        this.filterType = filterType;
        this.groupStrategyEnum = groupStrategyEnum;
        this.handler = handler;
    }

    public String getFilterType() {
        return filterType;
    }

    public GroupStrategyEnum getGroupStrategyEnum() {
        return groupStrategyEnum;
    }

    public Handler getHandler() {
        return handler;
    }
}