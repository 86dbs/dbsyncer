package org.dbsyncer.manager.template;

import org.dbsyncer.manager.config.OperationConfig;
import org.dbsyncer.manager.config.PreloadConfig;
import org.dbsyncer.manager.config.QueryConfig;
import org.dbsyncer.manager.enums.GroupStrategyEnum;

public abstract class AbstractTemplate {

    protected GroupStrategyEnum getDefaultStrategy(PreloadConfig config){
        return getDefaultStrategy(config.getGroupStrategyEnum());
    }

    protected GroupStrategyEnum getDefaultStrategy(OperationConfig config){
        return getDefaultStrategy(config.getGroupStrategyEnum());
    }

    protected GroupStrategyEnum getDefaultStrategy(QueryConfig query){
        return getDefaultStrategy(query.getGroupStrategyEnum());
    }

    private GroupStrategyEnum getDefaultStrategy(GroupStrategyEnum strategy){
        return null != strategy ? strategy : GroupStrategyEnum.DEFAULT;
    }

}