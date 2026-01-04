package org.dbsyncer.parser.model;

import org.dbsyncer.parser.enums.ConvertEnum;

/**
 * 字段转换
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/1 14:04
 */
public class Convert {

    /**
     * 字段名称
     */
    private String name;

    /**
     * 转换名称
     * @see ConvertEnum
     */
    private String convertName;

    /**
     * 转换方式
     *
     * @see ConvertEnum
     */
    private String convertCode;

    /**
     * 转换参数
     *
     * @see ConvertEnum
     */
    private String args;

    /**
     * 规则类型
     * - FUNCTION: 使用 ConvertEnum 的 Handler（默认）
     * - EXPRESSION: 表达式规则（如：${field1} + ' ' + ${field2}）
     * - FIXED: 固定值
     */
    private String ruleType = "FUNCTION";

    /**
     * 规则表达式（当 ruleType 为 EXPRESSION 或 FIXED 时使用）
     * 支持语法：${fieldName} 表示字段值，支持字符串拼接、数学运算等
     * 示例：
     * - ${first_name} + ' ' + ${last_name}
     * - ${price} * 1.1
     * - 'PREFIX_' + ${id}
     * - BATCH_001 (固定值)
     */
    private String ruleExpression;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getConvertName() {
        return convertName;
    }

    public void setConvertName(String convertName) {
        this.convertName = convertName;
    }

    public String getConvertCode() {
        return convertCode;
    }

    public void setConvertCode(String convertCode) {
        this.convertCode = convertCode;
    }

    public String getArgs() {
        return args;
    }

    public void setArgs(String args) {
        this.args = args;
    }

    public String getRuleType() {
        return ruleType;
    }

    public void setRuleType(String ruleType) {
        this.ruleType = ruleType;
    }

    public String getRuleExpression() {
        return ruleExpression;
    }

    public void setRuleExpression(String ruleExpression) {
        this.ruleExpression = ruleExpression;
    }
}