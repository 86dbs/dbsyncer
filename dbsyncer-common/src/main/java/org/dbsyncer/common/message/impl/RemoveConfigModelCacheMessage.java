/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.message.impl;

import org.dbsyncer.common.enums.CommonMessageTypeEnum;
import org.dbsyncer.common.message.CommonMessage;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-09-23 00:27
 */
public final class RemoveConfigModelCacheMessage implements CommonMessage {

    private String id;
    private String configModelType;

    @Override
    public CommonMessageTypeEnum getType() {
        return CommonMessageTypeEnum.REMOVE_CONFIG_MODEL_CACHE;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getConfigModelType() {
        return configModelType;
    }

    public void setConfigModelType(String configModelType) {
        this.configModelType = configModelType;
    }
}
