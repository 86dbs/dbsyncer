package org.dbsyncer.listener.mysql.net.impl.packet.command;

import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;
import org.dbsyncer.listener.mysql.io.util.XSerializer;

import java.io.IOException;

public class ComBinlogDumpPacket extends AbstractCommandPacket {
    private static final long serialVersionUID = 449639496684376511L;

    private long binlogPosition;
    private int binlogFlag;
    private long serverId;
    private StringColumn binlogFileName;

    public ComBinlogDumpPacket() {
        super(MySQLConstants.COM_BINLOG_DUMP);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("binlogPosition", binlogPosition)
                .append("binlogFlag", binlogFlag)
                .append("serverId", serverId)
                .append("binlogFileName", binlogFileName).toString();
    }

    public byte[] getPacketBody() throws IOException {
        final XSerializer ps = new XSerializer();
        ps.writeInt(this.command, 1);
        ps.writeLong(this.binlogPosition, 4);
        ps.writeInt(this.binlogFlag, 2);
        ps.writeLong(this.serverId, 4);
        ps.writeFixedLengthString(this.binlogFileName);
        return ps.toByteArray();
    }

    public long getBinlogPosition() {
        return binlogPosition;
    }

    public void setBinlogPosition(long binlogPosition) {
        this.binlogPosition = binlogPosition;
    }

    public int getBinlogFlag() {
        return binlogFlag;
    }

    public void setBinlogFlag(int binlogFlag) {
        this.binlogFlag = binlogFlag;
    }

    public long getServerId() {
        return serverId;
    }

    public void setServerId(long serverId) {
        this.serverId = serverId;
    }

    public StringColumn getBinlogFileName() {
        return binlogFileName;
    }

    public void setBinlogFileName(StringColumn binlogFileName) {
        this.binlogFileName = binlogFileName;
    }
}
