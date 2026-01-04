package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.SHA1Util;
import org.dbsyncer.parser.convert.AbstractHandler;

/**
 * SHA1加密
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:04
 */
public class Sha1Handler extends AbstractHandler {

    @Override
    protected Object convert(String args, Object value, java.util.Map<String, Object> row) {
        // row 参数未使用
        return SHA1Util.b64_sha1(String.valueOf(value));
    }
}