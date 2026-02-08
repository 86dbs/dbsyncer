/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.listener;

import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.plugin.AbstractPluginContext;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-05 01:07
 */
public final class QuartzListenerContext extends AbstractPluginContext {

    @Override
    public ModelEnum getModelEnum() {
        return ModelEnum.INCREMENT;
    }
}
