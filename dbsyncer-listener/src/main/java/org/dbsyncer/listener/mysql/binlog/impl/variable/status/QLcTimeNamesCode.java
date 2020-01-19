package org.dbsyncer.listener.mysql.binlog.impl.variable.status;

import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

public class QLcTimeNamesCode extends AbstractStatusVariable {
    public static final int TYPE = MySQLConstants.Q_LC_TIME_NAMES_CODE;

    private final int lcTimeNames;

    public QLcTimeNamesCode(int lcTimeNames) {
        super(TYPE);
        this.lcTimeNames = lcTimeNames;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("lcTimeNames", lcTimeNames).toString();
    }

    public int getLcTimeNames() {
        return lcTimeNames;
    }

    public static QLcTimeNamesCode valueOf(XInputStream tis) throws IOException {
        return new QLcTimeNamesCode(tis.readInt(2));
    }
}
