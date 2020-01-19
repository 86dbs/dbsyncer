package org.dbsyncer.listener.mysql.binlog.impl.parser;

import org.dbsyncer.listener.mysql.binlog.BinlogEventParser;
import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.binlog.BinlogParserContext;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

public final class NopEventParser implements BinlogEventParser {

    public int getEventType() {
        throw new UnsupportedOperationException();
    }

    public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context)
            throws IOException {
        final int available = is.available();
        is.skip(available);
    }
}
