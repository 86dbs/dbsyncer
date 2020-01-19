package org.dbsyncer.listener.mysql.binlog.impl.event;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;

/**
 * Written when mysqld stops.
 *
 * @ClassName: StopEvent
 * @author: AE86
 * @date: 2018年10月17日 下午2:29:15
 */
public final class StopEvent extends AbstractBinlogEventV4 {
    public static final int EVENT_TYPE = MySQLConstants.STOP_EVENT;

    public StopEvent() {
    }

    public StopEvent(BinlogEventV4Header header) {
        this.header = header;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("header", header).toString();
    }
}
