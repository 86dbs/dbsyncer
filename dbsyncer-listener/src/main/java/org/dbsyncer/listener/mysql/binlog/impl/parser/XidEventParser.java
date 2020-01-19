package org.dbsyncer.listener.mysql.binlog.impl.parser;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.binlog.BinlogParserContext;
import org.dbsyncer.listener.mysql.binlog.impl.event.XidEvent;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

public class XidEventParser extends AbstractBinlogEventParser {

    public XidEventParser() {
        super(XidEvent.EVENT_TYPE);
    }

    /**
     * Note: Contrary to all other numeric fields, the XID transaction number is not always
     * written in little-endian format. The bytes are copied unmodified from memory to disk,
     * so the format is machine-dependent. Hence, when replicating from a little-endian to a
     * big-endian machine (or vice versa), the numeric value of transaction numbers will differ.
     * In particular, the output of mysqlbinlog differs. This should does not cause inconsistencies
     * in replication because the only important property of transaction numbers is that different
     * transactions have different numbers (relative order does not matter).
     */
    public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context)
            throws IOException {
        final XidEvent event = new XidEvent(header);
        event.setBinlogFilename(context.getBinlogFileName());
        event.setXid(is.readLong(8));
        context.getEventListener().onEvents(event);
    }
}
