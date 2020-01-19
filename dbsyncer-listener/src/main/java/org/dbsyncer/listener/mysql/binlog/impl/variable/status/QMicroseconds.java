package org.dbsyncer.listener.mysql.binlog.impl.variable.status;

import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

public class QMicroseconds extends AbstractStatusVariable {
    public static final int TYPE = MySQLConstants.Q_MICROSECONDS;

    private final int startUsec;

    public QMicroseconds(int startUsec) {
        super(TYPE);
        this.startUsec = startUsec;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("startUsec", startUsec).toString();
    }

    public int getStartUsec() {
        return startUsec;
    }

    public static QMicroseconds valueOf(XInputStream tis) throws IOException {
        return new QMicroseconds(tis.readInt(3));
    }
}
