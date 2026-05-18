/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.enums;

import org.dbsyncer.common.util.StringUtil;

/**
 * 任务触发策略枚举
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2025-10-18 19:52
 */
public enum CommonTaskTriggerEnum {

    /**
     * 执行一次
     */
    ONCE("once", "执行一次"),

    /**
     * 定时执行
     */
    TIMING("timing", "定时执行");

    private final String code;
    private final String message;

    CommonTaskTriggerEnum(String code, String message) {
        this.code = code;
        this.message = message;
    }

    /**
     * 获取类型
     */
    public static CommonTaskTriggerEnum getType(String code) {
        for (CommonTaskTriggerEnum e : CommonTaskTriggerEnum.values()) {
            if (StringUtil.equals(code, e.getCode())) {
                return e;
            }
        }
        return null;
    }

    public static boolean isOnce(String code) {
        return ONCE.code.equals(code);
    }

    public static boolean isTiming(String code) {
        return TIMING.code.equals(code);
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
