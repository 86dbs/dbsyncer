package org.dbsyncer.sdk.enums;

import org.dbsyncer.common.util.StringUtil;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/24 14:19
 */
public enum ListenerTypeEnum {

    /**
     * 定时
     */
    TIMING("timing"),
    /**
     * 日志
     */
    LOG("log");

    private final String type;

    ListenerTypeEnum(String type) {
        this.type = type;
    }

    public static boolean isTiming(String type) {
        return StringUtil.equals(TIMING.getType(), type);
    }

    public static boolean isLog(String type) {
        return StringUtil.equals(LOG.getType(), type);
    }

    public String getType() {
        return type;
    }

}