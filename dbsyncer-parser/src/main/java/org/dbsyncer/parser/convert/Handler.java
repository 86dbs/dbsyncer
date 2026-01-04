package org.dbsyncer.parser.convert;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 22:55
 */
public interface Handler {

    /**
     * 值转换
     *
     * @param args 参数
     * @param value 值
     * @param row 数据行（可为 null，某些 Handler 不需要）
     * @return 转换后的值
     */
    Object handle(String args, Object value, Map<String, Object> row);
}