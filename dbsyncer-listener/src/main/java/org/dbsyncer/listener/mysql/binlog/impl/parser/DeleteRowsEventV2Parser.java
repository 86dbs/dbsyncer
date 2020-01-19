package org.dbsyncer.listener.mysql.binlog.impl.parser;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.binlog.BinlogParserContext;
import org.dbsyncer.listener.mysql.binlog.impl.event.DeleteRowsEventV2;
import org.dbsyncer.listener.mysql.binlog.impl.event.TableMapEvent;
import org.dbsyncer.listener.mysql.common.glossary.Row;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class DeleteRowsEventV2Parser extends AbstractRowEventParser {

    public DeleteRowsEventV2Parser() {
        super(DeleteRowsEventV2.EVENT_TYPE);
    }

    public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context)
            throws IOException {
        final long tableId = is.readLong(6);
        final TableMapEvent tme = context.getTableMapEvent(tableId);
        if (this.rowEventFilter != null && !this.rowEventFilter.accepts(header, context, tme)) {
            is.skip(is.available());
            return;
        }

        final DeleteRowsEventV2 event = new DeleteRowsEventV2(header);
        event.setBinlogFilename(context.getBinlogFileName());
        event.setTableId(tableId);
        event.setReserved(is.readInt(2));
        event.setExtraInfoLength(is.readInt(2));
        if (event.getExtraInfoLength() > 2) event.setExtraInfo(is.readBytes(event.getExtraInfoLength() - 2));
        event.setColumnCount(is.readUnsignedLong());
        event.setUsedColumns(is.readBit(event.getColumnCount().intValue()));
        event.setRows(parseRows(is, tme, event));
        context.getEventListener().onEvents(event);
    }

    protected List<Row> parseRows(XInputStream is, TableMapEvent tme, DeleteRowsEventV2 dre)
            throws IOException {
        final List<Row> r = new LinkedList<Row>();
        while (is.available() > 0) {
            r.add(parseRow(is, tme, dre.getUsedColumns()));
        }
        return r;
    }
}
