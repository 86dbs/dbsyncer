/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

/**
 * 条件表达式类型
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/26 23:21
 */
public enum OperationEnum {

    AND("and", "并且"),
    OR("or", "或者"),
    SQL("sql", "自定义sql条件");

    private final String name;
    private final String message;

    OperationEnum(String name,String message) {
        this.name = name;
        this.message = message;
    }

    public static boolean isAnd(String name) {
        return AND.getName().equals(name);
    }

    public static boolean isOr(String name) {
        return OR.getName().equals(name);
    }

    public String getName() {
        return name;
    }

    public String getMessage() {
        return message;
    }

}