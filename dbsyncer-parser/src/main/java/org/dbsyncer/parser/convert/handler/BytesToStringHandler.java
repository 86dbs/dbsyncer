package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.convert.AbstractHandler;

/**
 * Byte[]转String
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/12/20 23:04
 */
public class BytesToStringHandler extends AbstractHandler {

    @Override
    public Object convert(String args, Object value) {
        if (value instanceof byte[]) {
            byte[] b = (byte[]) value;
            value = new String(b);
        }
        return value;
    }
}
