package org.dbsyncer.listener.enums;

/**
 * 支持监听器类型
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/19 23:56
 */
public enum ListenerEnum {

    /**
     * 日志
     */
    LOG("Log"),
    /**
     * 定时
     */
    POLLING("Polling");

    // 策略编码
    private String code;

    ListenerEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

}