package org.dbsyncer.listener.mysql.binlog.impl.parser;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.binlog.BinlogParserContext;
import org.dbsyncer.listener.mysql.binlog.impl.event.RandEvent;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

public class RandEventParser extends AbstractBinlogEventParser {

    public RandEventParser() {
        super(RandEvent.EVENT_TYPE);
    }

    public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context)
            throws IOException {
        final RandEvent event = new RandEvent(header);
        event.setBinlogFilename(context.getBinlogFileName());
        event.setRandSeed1(is.readLong(8));
        event.setRandSeed2(is.readLong(8));
        context.getEventListener().onEvents(event);
    }
}
