package org.dbsyncer.listener.mysql.binlog.impl.variable.user;

import org.dbsyncer.listener.mysql.binlog.UserVariable;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;

public abstract class AbstractUserVariable implements UserVariable {
    protected final int type;

    public AbstractUserVariable(int type) {
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
