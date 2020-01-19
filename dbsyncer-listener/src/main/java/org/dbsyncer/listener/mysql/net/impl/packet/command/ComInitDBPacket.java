package org.dbsyncer.listener.mysql.net.impl.packet.command;

import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;
import org.dbsyncer.listener.mysql.io.util.XSerializer;

import java.io.IOException;

public class ComInitDBPacket extends AbstractCommandPacket {
    private static final long serialVersionUID = 449639496684376511L;

    private StringColumn databaseName;

    public ComInitDBPacket() {
        super(MySQLConstants.COM_INIT_DB);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("databaseName", databaseName).toString();
    }

    public byte[] getPacketBody() throws IOException {
        final XSerializer ps = new XSerializer();
        ps.writeInt(this.command, 1);
        ps.writeFixedLengthString(this.databaseName);
        return ps.toByteArray();
    }

    public StringColumn getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(StringColumn databaseName) {
        this.databaseName = databaseName;
    }
}
