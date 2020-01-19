package org.dbsyncer.listener.mysql.binlog.impl.parser;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.binlog.BinlogParserContext;
import org.dbsyncer.listener.mysql.binlog.impl.event.RotateEvent;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

public class RotateEventParser extends AbstractBinlogEventParser {

    public RotateEventParser() {
        super(RotateEvent.EVENT_TYPE);
    }

    public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context)
            throws IOException {
        final RotateEvent event = new RotateEvent(header);
        event.setBinlogFilename(context.getBinlogFileName());
        event.setBinlogPosition(is.readLong(8));
        event.setBinlogFileName(is.readFixedLengthString(is.available()));
        context.getEventListener().onEvents(event);
    }
}
