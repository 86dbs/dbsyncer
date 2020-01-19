package org.dbsyncer.listener.mysql.binlog.impl.parser;

import org.dbsyncer.listener.mysql.binlog.BinlogEventParser;

public abstract class AbstractBinlogEventParser implements BinlogEventParser {
    protected final int eventType;

    public AbstractBinlogEventParser(int eventType) {
        this.eventType = eventType;
    }

    public final int getEventType() {
        return eventType;
    }
}
