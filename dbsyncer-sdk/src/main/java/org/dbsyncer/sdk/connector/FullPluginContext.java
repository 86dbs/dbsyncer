package org.dbsyncer.sdk.connector;

import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.filter.BooleanFilter;
import org.dbsyncer.sdk.plugin.AbstractPluginContext;
import org.dbsyncer.sdk.plugin.CustomContext;

/**
 * @author AE86
 * @version 1.0.0s
 * @date 2022/6/30 16:04
 */
public final class FullPluginContext extends AbstractPluginContext implements CustomContext {

    private BooleanFilter booleanFilter = new BooleanFilter();

    private boolean targetConnector = false;

    @Override
    public ModelEnum getModelEnum() {
        return ModelEnum.FULL;
    }

    @Override
    public boolean isTargetConnector() {
        return targetConnector;
    }

    @Override
    public BooleanFilter getFilter() {
        return booleanFilter;
    }

    public void setBooleanFilter(BooleanFilter booleanFilter) {
        this.booleanFilter = booleanFilter;
    }

    public void setTargetConnector(boolean targetConnector) {
        this.targetConnector = targetConnector;
    }
}
