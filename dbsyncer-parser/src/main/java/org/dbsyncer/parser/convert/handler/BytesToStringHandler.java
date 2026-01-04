package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.convert.Handler;

import java.util.Map;

/**
 * Byte[]转String
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/12/20 23:04
 */
public class BytesToStringHandler implements Handler {

    @Override
    public Object handle(String args, Object value, Map<String, Object> row) {
        // row 参数未使用
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            byte[] b = (byte[]) value;
            return new String(b);
        }
        return value;
    }

}