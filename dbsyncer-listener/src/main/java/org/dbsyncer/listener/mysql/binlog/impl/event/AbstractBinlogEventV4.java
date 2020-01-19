package org.dbsyncer.listener.mysql.binlog.impl.event;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4;
import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;

public abstract class AbstractBinlogEventV4 implements BinlogEventV4 {
    protected BinlogEventV4Header header;
    protected String binlogFilename;

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("header", header).toString();
    }

    public BinlogEventV4Header getHeader() {
        return header;
    }

    public void setHeader(BinlogEventV4Header header) {
        this.header = header;
    }

    public String getBinlogFilename() {
        return binlogFilename;
    }

    public void setBinlogFilename(String binlogFilename) {
        this.binlogFilename = binlogFilename;
    }

}
