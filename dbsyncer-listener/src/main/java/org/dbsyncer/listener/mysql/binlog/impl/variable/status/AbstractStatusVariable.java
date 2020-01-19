package org.dbsyncer.listener.mysql.binlog.impl.variable.status;

import org.dbsyncer.listener.mysql.binlog.StatusVariable;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;

public abstract class AbstractStatusVariable implements StatusVariable {
    protected final int type;

    public AbstractStatusVariable(int type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).toString();
    }

    public int getType() {
        return type;
    }
}
