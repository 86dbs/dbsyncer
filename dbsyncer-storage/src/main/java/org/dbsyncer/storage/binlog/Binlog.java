package org.dbsyncer.storage.binlog;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/19 23:03
 */
public final class Binlog {
    private String fileName;
    private long position = 0;

    public String getFileName() {
        return fileName;
    }

    public Binlog setFileName(String fileName) {
        this.fileName = fileName;
        return this;
    }

    public long getPosition() {
        return position;
    }

    public Binlog setPosition(long position) {
        this.position = position;
        return this;
    }
}