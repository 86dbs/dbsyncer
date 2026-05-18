/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.enums;

import org.dbsyncer.common.util.StringUtil;

/**
 * 任务类型枚举
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-03-22 19:52
 */
public enum CommonTaskTypeEnum {

    /**
     * 订正校验
     */
    VALIDATE_SYNC;

    /**
     * 按名称解析任务类型，并统一异常语义。
     *
     * @param typeStr 任务类型字符串
     * @return 任务类型枚举
     */
    public static CommonTaskTypeEnum parse(String typeStr) {
        if (StringUtil.isBlank(typeStr)) {
            return null;
        }
        try {
            return CommonTaskTypeEnum.valueOf(typeStr);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

}