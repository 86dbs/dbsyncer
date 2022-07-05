package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.parser.convert.AbstractHandler;

import java.sql.Timestamp;

/**
 * Timestamp转中国标准时间
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/12/20 23:04
 */
public class TimestampToChineseStandardTimeHandler extends AbstractHandler {

    @Override
    public Object convert(String args, Object value) {
        if (value instanceof Timestamp) {
            Timestamp t = (Timestamp) value;
            value = DateFormatUtil.timestampToString(t);
        }
        return value;
    }

}