package org.dbsyncer.plugin.impl;

import org.dbsyncer.sdk.plugin.AbstractPluginContext;
import org.dbsyncer.sdk.enums.ModelEnum;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/30 16:06
 */
public final class IncrementPluginContext extends AbstractPluginContext {

    @Override
    public ModelEnum getModelEnum() {
        return ModelEnum.INCREMENT;
    }
}