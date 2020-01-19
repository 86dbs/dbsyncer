package org.dbsyncer.listener.mysql.binlog;

import org.dbsyncer.listener.mysql.binlog.impl.event.TableMapEvent;

public interface BinlogParserContext {
    boolean getChecksumEnabled();

    void setChecksumEnabled(boolean flag);


    String getBinlogFileName();

    BinlogEventListener getEventListener();

    TableMapEvent getTableMapEvent(long tableId);
}
