/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.constant;

import org.dbsyncer.connector.postgresql.enums.MessageDecoderEnum;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-12-29 00:21
 */
public final class PostgreSQLConfigConstant {

    /**
     * 插件 {@link MessageDecoderEnum}
     */
    public static final String PLUGIN_NAME = "pluginName";

    /**
     * 删除slot，增量同步，停止驱动自动删除Slot
     */
    public static final String DROP_SLOT_ON_CLOSE = "dropSlotOnClose";
}
