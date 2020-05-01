package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.AESEncyptUtil;
import org.dbsyncer.parser.convert.AbstractHandler;

/**
 * AES解密
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:04
 */
public class AesDecryptHandler extends AbstractHandler {

    @Override
    public Object convert(String args, Object value) throws Exception {
        return AESEncyptUtil.decrypt(String.valueOf(value), args);
    }

}