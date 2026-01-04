package org.dbsyncer.parser.util;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.convert.Handler;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.model.Convert;

import java.util.List;
import java.util.Map;

public abstract class ConvertUtil {

    private ConvertUtil() {
    }

    /**
     * 转换参数（批量处理）
     *
     * @param convert 转换配置列表
     * @param data    数据列表
     */
    public static void convert(List<Convert> convert, List<Map> data) {
        if (!CollectionUtils.isEmpty(convert) && !CollectionUtils.isEmpty(data)) {
            data.forEach(row -> convert(convert, row));
        }
    }

    /**
     * 转换参数（增强：支持字段值生成和转换，支持表达式规则）
     *
     * @param convert 转换配置列表
     * @param row     数据行
     */
    public static void convert(List<Convert> convert, Map row) {
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
            value = handler.handle(args, value, row);

            // 将处理后的值放入 Map（如果为 null 也放入，由 Handler 决定是否返回 null）
            row.put(name, value);
        }
    }
}