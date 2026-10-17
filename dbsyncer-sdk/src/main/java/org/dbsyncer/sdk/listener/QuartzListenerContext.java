/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.listener;

import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.plugin.AbstractPluginContext;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2024-12-05 01:07
 */
public final class QuartzListenerContext extends AbstractPluginContext {

    @Override
    public ModelEnum getModelEnum() {
        return ModelEnum.INCREMENT;
    }
}
