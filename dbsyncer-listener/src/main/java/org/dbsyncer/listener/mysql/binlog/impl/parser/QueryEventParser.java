package org.dbsyncer.listener.mysql.binlog.impl.parser;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.binlog.BinlogParserContext;
import org.dbsyncer.listener.mysql.binlog.StatusVariable;
import org.dbsyncer.listener.mysql.binlog.impl.event.QueryEvent;
import org.dbsyncer.listener.mysql.binlog.impl.variable.status.*;
import org.dbsyncer.listener.mysql.io.XInputStream;
import org.dbsyncer.listener.mysql.io.util.XDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class QueryEventParser extends AbstractBinlogEventParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryEventParser.class);

    public QueryEventParser() {
        super(QueryEvent.EVENT_TYPE);
    }

    public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context)
            throws IOException {
        final QueryEvent event = new QueryEvent(header);
        event.setBinlogFilename(context.getBinlogFileName());
        event.setThreadId(is.readLong(4));
        event.setElapsedTime(is.readLong(4));
        event.setDatabaseNameLength(is.readInt(1));
        event.setErrorCode(is.readInt(2));
        event.setStatusVariablesLength(is.readInt(2));
        event.setStatusVariables(parseStatusVariables(is.readBytes(event.getStatusVariablesLength())));
        event.setDatabaseName(is.readNullTerminatedString());
        event.setSql(is.readFixedLengthString(is.available()));
        context.getEventListener().onEvents(event);
    }

    protected List<StatusVariable> parseStatusVariables(byte[] data)
            throws IOException {
        final List<StatusVariable> r = new ArrayList<StatusVariable>();
        final XDeserializer d = new XDeserializer(data);
        boolean abort = false;
        while (!abort && d.available() > 0) {
            final int type = d.readInt(1);
            switch (type) {
                case QAutoIncrement.TYPE:
                    r.add(QAutoIncrement.valueOf(d));
                    break;
                case QCatalogCode.TYPE:
                    r.add(QCatalogCode.valueOf(d));
                    break;
                case QCatalogNzCode.TYPE:
                    r.add(QCatalogNzCode.valueOf(d));
                    break;
                case QCharsetCode.TYPE:
                    r.add(QCharsetCode.valueOf(d));
                    break;
                case QCharsetDatabaseCode.TYPE:
                    r.add(QCharsetDatabaseCode.valueOf(d));
                    break;
                case QFlags2Code.TYPE:
                    r.add(QFlags2Code.valueOf(d));
                    break;
                case QLcTimeNamesCode.TYPE:
                    r.add(QLcTimeNamesCode.valueOf(d));
                    break;
                case QSQLModeCode.TYPE:
                    r.add(QSQLModeCode.valueOf(d));
                    break;
                case QTableMapForUpdateCode.TYPE:
                    r.add(QTableMapForUpdateCode.valueOf(d));
                    break;
                case QTimeZoneCode.TYPE:
                    r.add(QTimeZoneCode.valueOf(d));
                    break;
                case QMasterDataWrittenCode.TYPE:
                    r.add(QMasterDataWrittenCode.valueOf(d));
                    break;
                case QInvoker.TYPE:
                    r.add(QInvoker.valueOf(d));
                    break;
                case QUpdatedDBNames.TYPE:
                    r.add(QUpdatedDBNames.valueOf(d));
                    break;
                case QMicroseconds.TYPE:
                    r.add(QMicroseconds.valueOf(d));
                    break;
                default:
                    LOGGER.warn("unknown status variable type: " + type);
                    abort = true;
                    break;
            }
        }
        return r;
    }
}
