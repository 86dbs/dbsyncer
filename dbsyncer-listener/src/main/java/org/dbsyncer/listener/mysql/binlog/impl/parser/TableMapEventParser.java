package org.dbsyncer.listener.mysql.binlog.impl.parser;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.binlog.BinlogParserContext;
import org.dbsyncer.listener.mysql.binlog.impl.event.TableMapEvent;
import org.dbsyncer.listener.mysql.common.glossary.Metadata;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

public class TableMapEventParser extends AbstractBinlogEventParser {
    private boolean reusePreviousEvent = true;

    public TableMapEventParser() {
        super(TableMapEvent.EVENT_TYPE);
    }

    public boolean isReusePreviousEvent() {
        return reusePreviousEvent;
    }

    public void setReusePreviousEvent(boolean reusePreviousEvent) {
        this.reusePreviousEvent = reusePreviousEvent;
    }

    public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context)
            throws IOException {
        final long tableId = is.readLong(6);
        if (this.reusePreviousEvent && context.getTableMapEvent(tableId) != null) {
            is.skip(is.available());
            final TableMapEvent event = context.getTableMapEvent(tableId).copy();
            event.setHeader(header);
            event.setBinlogFilename(context.getBinlogFileName());
            context.getEventListener().onEvents(event);
            return;
        }

        final TableMapEvent event = new TableMapEvent(header);
        event.setBinlogFilename(context.getBinlogFileName());
        event.setTableId(tableId);
        event.setReserved(is.readInt(2));
        event.setDatabaseNameLength(is.readInt(1));
        event.setDatabaseName(is.readNullTerminatedString());
        event.setTableNameLength(is.readInt(1));
        event.setTableName(is.readNullTerminatedString());
        event.setColumnCount(is.readUnsignedLong());
        event.setColumnTypes(is.readBytes(event.getColumnCount().intValue()));
        event.setColumnMetadataCount(is.readUnsignedLong());
        event.setColumnMetadata(Metadata.valueOf(event.getColumnTypes(), is.readBytes(event.getColumnMetadataCount().intValue())));
        event.setColumnNullabilities(is.readBit(event.getColumnCount().intValue()));
        context.getEventListener().onEvents(event);
    }
}
