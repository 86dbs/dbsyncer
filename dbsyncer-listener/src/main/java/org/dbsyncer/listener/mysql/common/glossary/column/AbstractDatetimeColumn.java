package org.dbsyncer.listener.mysql.common.glossary.column;

import org.dbsyncer.listener.mysql.common.glossary.Column;

import java.sql.Timestamp;

public abstract class AbstractDatetimeColumn implements Column {

    private static final long serialVersionUID = 4281176377064341058L;

    protected Timestamp timestampValue;

    public Timestamp getTimestampValue() {
        return this.timestampValue;
    }
}
