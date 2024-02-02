package org.dbsyncer.sdk.enums;

/**
 * 变更事件类型枚举
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-09 20:34
 */
public enum ChangedEventTypeEnum {
    /**
     * ddl变更
     */
    DDL,
    /**
     * 定时变更
     */
    SCAN,
    /**
     * 表行变更，比如mysql, 会获取变动行所有列的值
     */
    ROW;

    public static boolean isDDL(ChangedEventTypeEnum event) {
        return event != null && DDL == event;
    }

}