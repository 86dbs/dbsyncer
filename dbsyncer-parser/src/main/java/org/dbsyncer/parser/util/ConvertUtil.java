package org.dbsyncer.parser.util;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.Convert;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.convert.Handler;
import org.dbsyncer.parser.convert.handler.ExpressionHandler;
import org.dbsyncer.sdk.model.Field;

import java.util.List;
import java.util.Map;

public abstract class ConvertUtil {

    private ConvertUtil() {
    }

    /**
     * 转换参数（批量处理）
     *
     * @param convert 转换配置列表
     * @param data 数据列表
     */
    public static void convert(List<Convert> convert, List<Map> data) {
        if (!CollectionUtils.isEmpty(convert) && !CollectionUtils.isEmpty(data)) {
            data.forEach(row -> convert(convert, row, null));
        }
    }

    /**
     * 转换参数（批量处理，增强版）
     *
     * @param convert 转换配置列表
     * @param data 数据列表
     * @param targetFields 目标字段列表（用于检查字段是否存在）
     */
    public static void convert(List<Convert> convert, List<Map> data, List<Field> targetFields) {
        if (!CollectionUtils.isEmpty(convert) && !CollectionUtils.isEmpty(data)) {
            data.forEach(row -> convert(convert, row, targetFields));
        }
    }

    /**
     * 转换参数
     *
     * @param convert 转换配置列表
     * @param row 数据行
     */
    public static void convert(List<Convert> convert, Map row) {
        convert(convert, row, null);
    }

    /**
     * 转换参数（增强：支持字段值生成和转换，支持表达式规则）
     *
     * @param convert 转换配置列表
     * @param row 数据行
     * @param targetFields 目标字段列表（用于检查字段是否存在）
     */
    public static void convert(List<Convert> convert, Map row, List<Field> targetFields) {
        if (CollectionUtils.isEmpty(convert) || row == null) {
            return;
        }

        final int size = convert.size();
        Convert c = null;
        String name = null;
        String code = null;
        String args = null;
        Object value = null;

        for (int i = 0; i < size; i++) {
            c = convert.get(i);
            name = c.getName();

            // 获取转换类型
            code = c.getConvertCode();
            if (StringUtil.isBlank(code)) {
                continue;
            }

            ConvertEnum convertEnum = null;
            try {
                convertEnum = ConvertEnum.valueOf(code);
            } catch (Exception e) {
                // 如果 code 不是有效的枚举值，跳过
                continue;
            }

            // 获取参数（统一从 args 中读取）
            args = c.getArgs();

            // 获取 Handler
            Handler handler = convertEnum.getHandler();

            // 统一使用 Handler 处理
            value = row.get(name);

            // 如果字段不在 Map 中，检查是否在 targetFields 中
            if (value == null && containsField(targetFields, name)) {
                // 字段在目标表中，但不在 Map 中（无源字段）
                value = handler.handle(args, null, row);
                row.put(name, value);
            } else if (value != null) {
                // 字段在 Map 中，进行转换
                value = handler.handle(args, value, row);
                row.put(name, value);
            } else {
                // 字段不在 Map 中，也不在 targetFields 中，但某些 Handler（如 EXPRESSION）可能仍然需要处理
                // 例如：EXPRESSION 不依赖原值，可以直接计算
                value = handler.handle(args, null, row);
                if (value != null) {
                    row.put(name, value);
                }
            }
        }
    }

    /**
     * 检查字段是否在目标字段列表中
     */
    private static boolean containsField(List<Field> targetFields, String fieldName) {
        if (CollectionUtils.isEmpty(targetFields) || StringUtil.isBlank(fieldName)) {
            return false;
        }
        return targetFields.stream()
            .anyMatch(f -> f != null && StringUtil.equals(f.getName(), fieldName));
    }

}