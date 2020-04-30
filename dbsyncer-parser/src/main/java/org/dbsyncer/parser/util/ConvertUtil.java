package org.dbsyncer.parser.util;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.parser.convert.Convert;
import org.dbsyncer.parser.enums.ConvertEnum;

import java.util.List;
import java.util.Map;

public abstract class ConvertUtil {

    private ConvertUtil() {
    }

    /**
     * 转换参数
     *
     * @param convert
     * @param data
     */
    public static void convert(List<Convert> convert, List<Map<String, Object>> data) {
        if (!CollectionUtils.isEmpty(convert) && !CollectionUtils.isEmpty(data)) {
            // 并行流计算
            final int size = convert.size();
            data.parallelStream().forEach(row -> {
                // 替换row值, 复用堆栈地址，减少开销
                Convert c = null;
                String name = null;
                String code = null;
                String args = null;
                Object value = null;
                for (int i = 0; i < size; i++) {
                    c = convert.get(i);
                    name = c.getName();
                    code = c.getConvertCode();
                    args = c.getArgs();
                    value = ConvertEnum.getHandler(code).handle(args, row.get(name));

                    row.put(name, value);
                }
            });
        }
    }

}