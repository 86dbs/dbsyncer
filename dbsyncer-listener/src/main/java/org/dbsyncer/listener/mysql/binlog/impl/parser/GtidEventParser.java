package org.dbsyncer.listener.mysql.binlog.impl.parser;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.binlog.BinlogParserContext;
import org.dbsyncer.listener.mysql.binlog.impl.event.GtidEvent;
import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

/**
 * <h3>GTID Event</h3>
 * <ol type="1">
 * <li><dt>Event format:</dt></li>
 * <pre>
 *    +-------------------+
 *    | 1B commit flag    |
 *    +-------------------+
 *    | 16B Source ID     |
 *    +-------------------+
 *    | 8B Txn ID         |
 *    +-------------------+
 *    | ...               |
 *    +-------------------+
 *  </pre>
 * </ol>
 *
 * @ClassName: GtidEventParser
 * @author: AE86
 * @date: 2018年10月17日 下午2:33:19
 */
public class GtidEventParser extends AbstractBinlogEventParser {

    public GtidEventParser() {
        super(MySQLConstants.GTID_LOG_EVENT);
    }

    public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException {
        GtidEvent event = new GtidEvent(header);
        is.readBytes(1); // commit flag, always true
        event.setSourceId(is.readBytes(16));
        event.setTransactionId(is.readLong(8, true));
        //event.setTransactionId(ByteBuffer.wrap(is.readBytes(8)).order(ByteOrder.LITTLE_ENDIAN).getLong());
        is.skip(is.available()); // position at next event
        context.getEventListener().onEvents(event);
    }
}
