package org.dbsyncer.parser.util;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.parser.convert.Convert;

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
            // TODO 参数转换
        }
    }

}