package org.dbsyncer.listener.mysql.binlog.impl.parser;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.binlog.BinlogParserContext;
import org.dbsyncer.listener.mysql.binlog.impl.event.IntvarEvent;
import org.dbsyncer.listener.mysql.common.glossary.UnsignedLong;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

public class IntvarEventParser extends AbstractBinlogEventParser {

    public IntvarEventParser() {
        super(IntvarEvent.EVENT_TYPE);
    }

    public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context)
            throws IOException {
        final IntvarEvent event = new IntvarEvent(header);
        event.setBinlogFilename(context.getBinlogFileName());
        event.setType(is.readInt(1));
        event.setValue(UnsignedLong.valueOf(is.readLong(8)));
        context.getEventListener().onEvents(event);
    }
}
