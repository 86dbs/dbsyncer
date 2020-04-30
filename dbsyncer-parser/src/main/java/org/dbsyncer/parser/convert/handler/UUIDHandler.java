package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.parser.convert.Handler;

/**
 * UUID
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:05
 */
public class UUIDHandler implements Handler {

    @Override
    public Object handle(String args, Object value) {
        return UUIDUtil.getUUID();
    }
}