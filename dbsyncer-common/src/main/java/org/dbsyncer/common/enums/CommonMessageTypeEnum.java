/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.enums;

/**
 * 消息业务域。
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-09-23 00:27
 */
public enum CommonMessageTypeEnum {

    /**
     * 删除配置缓存
     */
    REMOVE_CONFIG_MODEL_CACHE(0);

    private final int code;

    CommonMessageTypeEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
