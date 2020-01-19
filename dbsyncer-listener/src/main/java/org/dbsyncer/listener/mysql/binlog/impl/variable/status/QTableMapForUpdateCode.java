package org.dbsyncer.listener.mysql.binlog.impl.variable.status;

import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;
import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

public class QTableMapForUpdateCode extends AbstractStatusVariable {
    public static final int TYPE = MySQLConstants.Q_TABLE_MAP_FOR_UPDATE_CODE;

    private final long tableMap;

    public QTableMapForUpdateCode(long tableMap) {
        super(TYPE);
        this.tableMap = tableMap;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("tableMap", tableMap).toString();
    }

    public long getTableMap() {
        return tableMap;
    }

    public static QTableMapForUpdateCode valueOf(XInputStream tis) throws IOException {
        return new QTableMapForUpdateCode(tis.readLong(8));
    }
}
