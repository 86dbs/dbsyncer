/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz;

/**
 * 主键必需异常
 * 当数据源表或目标表缺少主键时抛出此异常
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2025-01-XX
 */
public class PrimaryKeyRequiredException extends BizException {

    public PrimaryKeyRequiredException(String message) {
        super(message);
    }

}
