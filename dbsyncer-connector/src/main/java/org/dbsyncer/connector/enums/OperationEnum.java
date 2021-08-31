package org.dbsyncer.connector.enums;

/**
 * 条件表达式类型
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/26 23:21
 */
public enum OperationEnum {

    /**
     * 并且
     */
    AND("and"),
    /**
     * 或者
     */
    OR("or");

    // 描述
    private String name;

    OperationEnum(String name) {
        this.name = name;
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

}