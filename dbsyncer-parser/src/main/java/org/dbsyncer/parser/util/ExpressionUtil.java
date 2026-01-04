package org.dbsyncer.parser.util;

import org.dbsyncer.common.util.StringUtil;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 表达式工具类
 *
 * @author DBSyncer
 * @version 1.0.0
 */
public abstract class ExpressionUtil {

    private static final Pattern FIELD_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");

    /**
     * 计算表达式
     *
     * @param expression 表达式（如：${field1} + ' ' + ${field2}）
     * @param row 数据行
     * @return 计算结果
     */
    public static Object evaluate(String expression, Map<String, Object> row) {
        if (StringUtil.isBlank(expression)) {
            return null;
        }

        // 替换字段占位符 ${fieldName} 为实际值
        String result = expression;
        Matcher matcher = FIELD_PATTERN.matcher(expression);

        while (matcher.find()) {
            String fieldName = matcher.group(1);
            Object fieldValue = row.get(fieldName);
            String valueStr = fieldValue != null ? String.valueOf(fieldValue) : "";

            // 转义特殊字符，避免在字符串拼接时出错
            valueStr = escapeForExpression(valueStr);

            result = result.replace("${" + fieldName + "}", valueStr);
        }

        // 简单的表达式计算（支持字符串拼接和基本数学运算）
        return evaluateSimpleExpression(result);
    }

    /**
     * 转义特殊字符
     */
    private static String escapeForExpression(String value) {
        if (value == null) {
            return "";
        }
        // 如果值包含单引号，需要转义
        return value.replace("'", "''");
    }

    /**
     * 简单的表达式计算，使用 JavaScript 引擎
     */
    private static Object evaluateSimpleExpression(String expression) {
        try {
            javax.script.ScriptEngineManager manager = new javax.script.ScriptEngineManager();
            javax.script.ScriptEngine engine = manager.getEngineByName("JavaScript");
            Object result = engine.eval(expression);
            return result;
        } catch (Exception e) {
            throw new RuntimeException("表达式计算失败: " + expression, e);
        }
    }
}

