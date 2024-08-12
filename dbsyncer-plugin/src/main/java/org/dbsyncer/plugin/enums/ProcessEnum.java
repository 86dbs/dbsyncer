/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.plugin.enums;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-08-13 00:27
 */
public enum ProcessEnum {

    /**
     * 全量同步前置处理
     */
    BEFORE,

    /**
     * 全量同步/增量同步转换
     */
    CONVERT,

    /**
     * 全量同步/增量同步后置处理
     */
    AFTER;

}