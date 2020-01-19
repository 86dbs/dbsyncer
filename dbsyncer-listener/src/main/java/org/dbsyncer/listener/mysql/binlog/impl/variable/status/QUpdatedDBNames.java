package org.dbsyncer.listener.mysql.binlog.impl.variable.status;

import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

public class QUpdatedDBNames extends AbstractStatusVariable {
    public static final int TYPE = MySQLConstants.Q_UPDATED_DB_NAMES;

    private final int accessedDbCount;
    private final StringColumn[] accessedDbs;

    public QUpdatedDBNames(int accessedDbCount, StringColumn[] accessedDbs) {
        super(TYPE);
        this.accessedDbCount = accessedDbCount;
        this.accessedDbs = accessedDbs;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("accessedDbCount", accessedDbCount)
                .append("accessedDbs", accessedDbs).toString();
    }

    public int getAccessedDbCount() {
        return accessedDbCount;
    }

    public StringColumn[] getAccessedDbs() {
        return accessedDbs;
    }

    public static QUpdatedDBNames valueOf(XInputStream tis) throws IOException {
        int accessedDbCount = tis.readInt(1);
        StringColumn accessedDbs[] = null;
        if (accessedDbCount > MySQLConstants.MAX_DBS_IN_EVENT_MTS) {
            accessedDbCount = MySQLConstants.OVER_MAX_DBS_IN_EVENT_MTS;
        } else {
            accessedDbs = new StringColumn[accessedDbCount];
            for (int i = 0; i < accessedDbCount; i++) {
                accessedDbs[i] = tis.readNullTerminatedString();
            }
        }
        return new QUpdatedDBNames(accessedDbCount, accessedDbs);
    }
}
