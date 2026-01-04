/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.AESUtil;
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
    public Object convert(String args, Object value, java.util.Map<String, Object> row) throws Exception {
        // row 参数未使用
        return AESUtil.decrypt(String.valueOf(value), args);
    }

}