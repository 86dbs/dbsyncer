package org.dbsyncer.storage.binlog;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/19 23:03
 */
public final class Binlog {
    private String binlog;
    private long pos = 0;

    public String getBinlog() {
        return binlog;
    }

    public Binlog setBinlog(String binlog) {
        this.binlog = binlog;
        return this;
    }

    public long getPos() {
        return pos;
    }

    public Binlog setPos(long pos) {
        this.pos = pos;
        return this;
    }
}
