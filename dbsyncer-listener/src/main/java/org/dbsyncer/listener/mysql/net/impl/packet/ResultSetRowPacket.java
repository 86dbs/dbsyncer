package org.dbsyncer.listener.mysql.net.impl.packet;

import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;
import org.dbsyncer.listener.mysql.io.util.XDeserializer;
import org.dbsyncer.listener.mysql.io.util.XSerializer;
import org.dbsyncer.listener.mysql.net.Packet;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class ResultSetRowPacket extends AbstractPacket {
    private static final long serialVersionUID = 698187140476020984L;

    private List<StringColumn> columns;

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("columns", columns).toString();
    }

    public byte[] getPacketBody() {
        final XSerializer s = new XSerializer(1024);
        for (StringColumn column : this.columns) {
            s.writeLengthCodedString(column);
        }
        return s.toByteArray();
    }

    public List<StringColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<StringColumn> columns) {
        this.columns = columns;
    }

    public static ResultSetRowPacket valueOf(Packet packet) throws IOException {
        final XDeserializer d = new XDeserializer(packet.getPacketBody());
        final ResultSetRowPacket r = new ResultSetRowPacket();
        r.length = packet.getLength();
        r.sequence = packet.getSequence();
        r.setColumns(new LinkedList<StringColumn>());
        while (d.available() > 0) {
            r.getColumns().add(d.readLengthCodedString());
        }
        return r;
    }
}
