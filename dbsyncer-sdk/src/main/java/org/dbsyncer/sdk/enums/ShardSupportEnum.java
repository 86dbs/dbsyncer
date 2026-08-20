/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * 连接器表内切片能力。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-20
 */
public enum ShardSupportEnum {

    /**
     * 不切分，整表一个 WorkItem
     */
    NONE,

    /**
     * 主键/排序键闭区间
     */
    RANGE,

    /**
     * 哈希取模
     */
    HASH_MOD,

    /**
     * 文件/流字节或行偏移
     */
    OFFSET,

    /**
     * 物理分区 / Topic 分区
     */
    PARTITION
}
