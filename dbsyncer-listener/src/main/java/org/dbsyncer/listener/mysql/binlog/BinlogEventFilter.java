package org.dbsyncer.listener.mysql.binlog;

public interface BinlogEventFilter {

	boolean accepts(BinlogEventV4Header header, BinlogParserContext context);
}
