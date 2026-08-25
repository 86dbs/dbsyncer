/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * 工作项边界类型（Leader 规划、Worker 按边界执行）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-25
 */
public enum WorkBoundType {

    /**
     * 整表一个工作项，无额外边界
     */
    NONE,

    /**
     * 游标分批：排他起始游标 + 行预算
     */
    CURSOR_BATCH,

    /**
     * 字节/行偏移区间
     */
    OFFSET,

    /**
     * 物理分区 / Topic 分区
     */
    PARTITION
}
