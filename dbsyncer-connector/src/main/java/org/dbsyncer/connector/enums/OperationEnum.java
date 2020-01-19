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
    AND("and", "and"),
    /**
     * 或者
     */
    OR("or", "or");

    // 描述
    private String name;

    // 运算符
    private String code;

    OperationEnum(String name, String code) {
        this.name = name;
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public String getCode() {
        return code;
    }

}