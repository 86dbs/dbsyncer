package org.dbsyncer.listener.mysql.binlog;

import org.dbsyncer.listener.mysql.binlog.impl.event.TableMapEvent;

public interface BinlogRowEventFilter {

    boolean accepts(BinlogEventV4Header header, BinlogParserContext context, TableMapEvent event);
}
