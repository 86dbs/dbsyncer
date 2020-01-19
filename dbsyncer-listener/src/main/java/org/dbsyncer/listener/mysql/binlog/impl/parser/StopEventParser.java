package org.dbsyncer.listener.mysql.binlog.impl.parser;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.binlog.BinlogParserContext;
import org.dbsyncer.listener.mysql.binlog.impl.event.StopEvent;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

public class StopEventParser extends AbstractBinlogEventParser {

    public StopEventParser() {
        super(StopEvent.EVENT_TYPE);
    }

    public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context)
            throws IOException {
        final StopEvent event = new StopEvent(header);
        event.setBinlogFilename(context.getBinlogFileName());
        context.getEventListener().onEvents(event);
    }
}
