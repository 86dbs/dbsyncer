package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.convert.Handler;

import java.util.Map;

/**
 * Number转String
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/7/20 23:04
 */
public class NumberToStringHandler implements Handler {

    @Override
    public Object handle(String args, Object value, Map<String, Object> row) {
        // row 参数未使用
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

}