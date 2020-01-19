package org.dbsyncer.listener.mysql.binlog.impl.variable.status;

import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

public class QAutoIncrement extends AbstractStatusVariable {
    public static final int TYPE = MySQLConstants.Q_AUTO_INCREMENT;

    private final int autoIncrementIncrement;
    private final int autoIncrementOffset;

    public QAutoIncrement(int autoIncrementIncrement, int autoIncrementOffset) {
        super(TYPE);
        this.autoIncrementIncrement = autoIncrementIncrement;
        this.autoIncrementOffset = autoIncrementOffset;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("autoIncrementIncrement", autoIncrementIncrement)
                .append("autoIncrementOffset", autoIncrementOffset).toString();
    }

    public int getAutoIncrementIncrement() {
        return autoIncrementIncrement;
    }

    public int getAutoIncrementOffset() {
        return autoIncrementOffset;
    }

    public static QAutoIncrement valueOf(XInputStream tis) throws IOException {
        final int autoIncrementIncrement = tis.readInt(2);
        final int autoIncrementOffset = tis.readInt(2);
        return new QAutoIncrement(autoIncrementIncrement, autoIncrementOffset);
    }
}
