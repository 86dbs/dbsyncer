package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.SHA1Util;
import org.dbsyncer.parser.convert.Handler;

import java.util.Map;

/**
 * SHA1加密
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:04
 */
public class Sha1Handler implements Handler {

    @Override
    public Object handle(String args, Object value, Map<String, Object> row) {
        // row 参数未使用
        if (value == null) {
            return null;
        }
        return SHA1Util.b64_sha1(String.valueOf(value));
    }
}