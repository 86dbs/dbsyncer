package org.dbsyncer.listener.mysql.binlog.impl.variable.user;

import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;

public class UserVariableReal extends AbstractUserVariable {
    public static final int TYPE = MySQLConstants.REAL_RESULT;

    private final double value;

    public UserVariableReal(double value) {
        super(TYPE);
        this.value = value;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("value", value).toString();
    }

    public Double getValue() {
        return this.value;
    }
}
