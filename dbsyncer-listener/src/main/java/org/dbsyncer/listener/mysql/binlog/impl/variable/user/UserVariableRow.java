package org.dbsyncer.listener.mysql.binlog.impl.variable.user;

import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;

public class UserVariableRow extends AbstractUserVariable {
    public static final int TYPE = MySQLConstants.ROW_RESULT;

    private final byte[] value;

    public UserVariableRow(byte[] value) {
        super(TYPE);
        this.value = value;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("value", value).toString();
    }

    public byte[] getValue() {
        return this.value;
    }
}
