package org.dbsyncer.listener.mysql.net.impl;

import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.dbsyncer.listener.mysql.net.Packet;
import org.dbsyncer.listener.mysql.net.Transport;
import org.dbsyncer.listener.mysql.net.TransportException;
import org.dbsyncer.listener.mysql.net.impl.packet.EOFPacket;
import org.dbsyncer.listener.mysql.net.impl.packet.ErrorPacket;
import org.dbsyncer.listener.mysql.net.impl.packet.ResultSetHeaderPacket;
import org.dbsyncer.listener.mysql.net.impl.packet.ResultSetRowPacket;
import org.dbsyncer.listener.mysql.net.impl.packet.command.ComQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Query {
    private final Transport transport;

    public Query(Transport transport) {
        this.transport = transport;
    }

    public List<String> getFirst(String sql) throws IOException, TransportException {
        List<String> result = null;

        final ComQuery command = new ComQuery();
        command.setSql(StringColumn.valueOf(sql.getBytes()));
        transport.getOutputStream().writePacket(command);
        transport.getOutputStream().flush();

        Packet packet = transport.getInputStream().readPacket();
        if (packet.getPacketBody()[0] == ErrorPacket.PACKET_MARKER) {
            throw new TransportException(ErrorPacket.valueOf(packet));
        }

        ResultSetHeaderPacket header = ResultSetHeaderPacket.valueOf(packet);
        if (header.getFieldCount().longValue() == 0) {
            return null;
        }

        while (true) {
            packet = transport.getInputStream().readPacket();
            if (packet.getPacketBody()[0] == EOFPacket.PACKET_MARKER) {
                break;
            }
        }

        while (true) {
            packet = transport.getInputStream().readPacket();
            if (packet.getPacketBody()[0] == EOFPacket.PACKET_MARKER) {
                break;
            } else {
                ResultSetRowPacket row = ResultSetRowPacket.valueOf(packet);
                if (result == null) {
                    result = new ArrayList<String>();

                    for (StringColumn c : row.getColumns()) {
                        result.add(c.toString());
                    }
                }
            }
        }
        return result;
    }
}
