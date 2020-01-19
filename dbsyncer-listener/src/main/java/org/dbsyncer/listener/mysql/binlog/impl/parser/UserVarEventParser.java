package org.dbsyncer.listener.mysql.binlog.impl.parser;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.binlog.BinlogParserContext;
import org.dbsyncer.listener.mysql.binlog.UserVariable;
import org.dbsyncer.listener.mysql.binlog.impl.event.UserVarEvent;
import org.dbsyncer.listener.mysql.binlog.impl.variable.user.*;
import org.dbsyncer.listener.mysql.io.XInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class UserVarEventParser extends AbstractBinlogEventParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserVarEventParser.class);

    public UserVarEventParser() {
        super(UserVarEvent.EVENT_TYPE);
    }

    public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context)
            throws IOException {
        final UserVarEvent event = new UserVarEvent(header);
        event.setBinlogFilename(context.getBinlogFileName());
        event.setVarNameLength(is.readInt(4));
        event.setVarName(is.readFixedLengthString(event.getVarNameLength()));
        event.setIsNull(is.readInt(1));
        if (event.getIsNull() == 0) {
            event.setVarType(is.readInt(1));
            event.setVarCollation(is.readInt(4));
            event.setVarValueLength(is.readInt(4));
            event.setVarValue(parseUserVariable(is, event));
        }
        context.getEventListener().onEvents(event);
    }

    protected UserVariable parseUserVariable(XInputStream is, UserVarEvent event)
            throws IOException {
        final int type = event.getVarType();
        switch (type) {
            case UserVariableDecimal.TYPE:
                return new UserVariableDecimal(is.readBytes(event.getVarValueLength()));
            case UserVariableInt.TYPE:
                return new UserVariableInt(is.readLong(event.getVarValueLength()), is.readInt(1));
            case UserVariableReal.TYPE:
                return new UserVariableReal(Double.longBitsToDouble(is.readLong(event.getVarValueLength())));
            case UserVariableRow.TYPE:
                return new UserVariableRow(is.readBytes(event.getVarValueLength()));
            case UserVariableString.TYPE:
                return new UserVariableString(is.readBytes(event.getVarValueLength()), event.getVarCollation());
            default:
                LOGGER.warn("unknown user variable type: " + type);
                return null;
        }
    }
}
