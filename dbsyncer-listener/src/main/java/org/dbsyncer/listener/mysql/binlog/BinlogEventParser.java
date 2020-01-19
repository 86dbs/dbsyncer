package org.dbsyncer.listener.mysql.binlog;

import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

public interface BinlogEventParser {

    int getEventType();

    void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException;
}
