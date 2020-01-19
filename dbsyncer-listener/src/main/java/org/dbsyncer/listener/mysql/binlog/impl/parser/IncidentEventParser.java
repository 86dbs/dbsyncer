package org.dbsyncer.listener.mysql.binlog.impl.parser;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.binlog.BinlogParserContext;
import org.dbsyncer.listener.mysql.binlog.impl.event.IncidentEvent;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

public class IncidentEventParser extends AbstractBinlogEventParser {

    public IncidentEventParser() {
        super(IncidentEvent.EVENT_TYPE);
    }

    public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context)
            throws IOException {
        final IncidentEvent event = new IncidentEvent(header);
        event.setBinlogFilename(context.getBinlogFileName());
        event.setIncidentNumber(is.readInt(1));
        event.setMessageLength(is.readInt(1));
        if (event.getMessageLength() > 0) event.setMessage(is.readFixedLengthString(event.getMessageLength()));
        context.getEventListener().onEvents(event);
    }
}
