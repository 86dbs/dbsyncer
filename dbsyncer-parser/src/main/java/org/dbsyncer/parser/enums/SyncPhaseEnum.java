package org.dbsyncer.parser.enums;

/**
 * 混合模式阶段枚举
 *
 * @author AE86
 * @version 1.0.0
 */
public enum SyncPhaseEnum {
    /**
     * 全量同步阶段
     */
    FULL,
    /**
     * 增量同步阶段
     */
    INCREMENTAL
}