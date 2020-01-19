package org.dbsyncer.listener.mysql.binlog.impl.variable.user;

import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;

public class UserVariableInt extends AbstractUserVariable {
    public static final int TYPE = MySQLConstants.INT_RESULT;

    private final long value;
    private final int obj;

    public UserVariableInt(long value, int obj) {
        super(TYPE);
        this.value = value;
        this.obj = obj;
    }

    public String toString() {
        return new ToStringBuilder(this)
                .append("value", value).toString();
    }

    public Long getValue() {
        return this.value;
    }

    public int getObj() {
        return obj;
    }

}
