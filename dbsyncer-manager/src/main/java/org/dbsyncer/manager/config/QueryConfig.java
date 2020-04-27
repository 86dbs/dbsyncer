package org.dbsyncer.manager.config;

import org.dbsyncer.manager.enums.GroupStrategyEnum;
import org.dbsyncer.parser.model.ConfigModel;

public class QueryConfig<T> {

    private ConfigModel configModel;

    private GroupStrategyEnum groupStrategyEnum;

    public QueryConfig(ConfigModel configModel) {
        this.configModel = configModel;
    }

    public QueryConfig(ConfigModel configModel, GroupStrategyEnum groupStrategyEnum) {
        this.configModel = configModel;
        this.groupStrategyEnum = groupStrategyEnum;
    }

    public ConfigModel getConfigModel() {
        return configModel;
    }

    public GroupStrategyEnum getGroupStrategyEnum() {
        return groupStrategyEnum;
    }
}