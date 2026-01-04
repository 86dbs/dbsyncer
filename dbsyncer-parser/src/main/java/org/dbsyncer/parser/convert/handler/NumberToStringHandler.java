package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.convert.AbstractHandler;

/**
 * Number转String
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/7/20 23:04
 */
public class NumberToStringHandler extends AbstractHandler {

    @Override
    public Object convert(String args, Object value, java.util.Map<String, Object> row) {
        // row 参数未使用
        return String.valueOf(value);
    }

}