package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.convert.Handler;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:05
 */
public class ClearHandler implements Handler {

    @Override
    public Object handle(String args, Object value, Map<String, Object> row) {
        // row 参数未使用
        return null;
    }
}