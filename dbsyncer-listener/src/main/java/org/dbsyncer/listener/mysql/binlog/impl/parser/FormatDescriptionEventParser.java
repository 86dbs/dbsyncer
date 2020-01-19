package org.dbsyncer.listener.mysql.binlog.impl.parser;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.binlog.BinlogParserContext;
import org.dbsyncer.listener.mysql.binlog.impl.event.FormatDescriptionEvent;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

public class FormatDescriptionEventParser extends AbstractBinlogEventParser {

    public FormatDescriptionEventParser() {
        super(FormatDescriptionEvent.EVENT_TYPE);
    }

    public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context)
            throws IOException {
        final FormatDescriptionEvent event = new FormatDescriptionEvent(header);
        event.setBinlogFilename(context.getBinlogFileName());
        event.setBinlogVersion(is.readInt(2));
        event.setServerVersion(is.readFixedLengthString(50));
        event.setCreateTimestamp(is.readLong(4) * 1000L);
        event.setHeaderLength(is.readInt(1));

        int eventTypeLength = (int) (event.getHeader().getEventLength() - (event.getHeaderLength() + 57));
        byte[] eventTypeBuffer;

        if (event.checksumPossible()) {
            eventTypeBuffer = is.readBytes(eventTypeLength - 4);
        } else {
            eventTypeBuffer = is.readBytes(eventTypeLength);
        }

        event.setEventTypes(eventTypeBuffer);

        // for mysql 5.6, there will always be space for a checksum in the FormatDescriptionEvent, even if checksums are off,
        // but our checksumming code will not have been active, so we don't bother to verify the checksum
        // of the formatLogDescription event.

        if (event.checksumPossible()) {
            is.readBytes(4);
        }

        context.setChecksumEnabled(event.checksumEnabled());
        context.getEventListener().onEvents(event);
    }
}
