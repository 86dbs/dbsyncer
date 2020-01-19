package org.dbsyncer.listener.mysql.net.impl.packet.command;

import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;
import org.dbsyncer.listener.mysql.io.util.XSerializer;

import java.io.IOException;

public class ComQuery extends AbstractCommandPacket {
    private static final long serialVersionUID = 1580858690926781520L;

    private StringColumn sql;

    public ComQuery() {
        super(MySQLConstants.COM_QUERY);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("sql", sql).toString();
    }

    public byte[] getPacketBody() throws IOException {
        final XSerializer ps = new XSerializer();
        ps.writeInt(this.command, 1);
        ps.writeFixedLengthString(this.sql);
        return ps.toByteArray();
    }

    public StringColumn getSql() {
        return sql;
    }

    public void setSql(StringColumn sql) {
        this.sql = sql;
    }
}
