package org.dbsyncer.listener.mysql.binlog.impl.parser;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.binlog.BinlogParserContext;
import org.dbsyncer.listener.mysql.binlog.impl.event.TableMapEvent;
import org.dbsyncer.listener.mysql.binlog.impl.event.UpdateRowsEventV2;
import org.dbsyncer.listener.mysql.common.glossary.Pair;
import org.dbsyncer.listener.mysql.common.glossary.Row;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class UpdateRowsEventV2Parser extends AbstractRowEventParser {

    public UpdateRowsEventV2Parser() {
        super(UpdateRowsEventV2.EVENT_TYPE);
    }

    public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context)
            throws IOException {
        final long tableId = is.readLong(6);
        final TableMapEvent tme = context.getTableMapEvent(tableId);
        if (this.rowEventFilter != null && !this.rowEventFilter.accepts(header, context, tme)) {
            is.skip(is.available());
            return;
        }

        final UpdateRowsEventV2 event = new UpdateRowsEventV2(header);
        event.setBinlogFilename(context.getBinlogFileName());
        event.setTableId(tableId);
        event.setReserved(is.readInt(2));
        event.setExtraInfoLength(is.readInt(2));
        if (event.getExtraInfoLength() > 2) event.setExtraInfo(is.readBytes(event.getExtraInfoLength() - 2));
        event.setColumnCount(is.readUnsignedLong());
        event.setUsedColumnsBefore(is.readBit(event.getColumnCount().intValue()));
        event.setUsedColumnsAfter(is.readBit(event.getColumnCount().intValue()));
        event.setRows(parseRows(is, tme, event));
        context.getEventListener().onEvents(event);
    }

    protected List<Pair<Row>> parseRows(XInputStream is, TableMapEvent tme, UpdateRowsEventV2 ure)
            throws IOException {
        final List<Pair<Row>> r = new LinkedList<Pair<Row>>();
        while (is.available() > 0) {
            final Row before = parseRow(is, tme, ure.getUsedColumnsBefore());
            final Row after = parseRow(is, tme, ure.getUsedColumnsAfter());
            r.add(new Pair<Row>(before, after));
        }
        return r;
    }
}
