package org.dbsyncer.storage.model;

import org.dbsyncer.storage.binlog.BinlogActuator;
import org.dbsyncer.storage.enums.BinlogStatusEnum;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/26 22:48
 */
public class BinlogIndex {

    private String fileName;
    private BinlogStatusEnum status;
    private Set<BinlogActuator> lock;
    private long createTime;
    private long updateTime;

    public BinlogIndex(String fileName) {
        this.fileName = fileName;
        status = BinlogStatusEnum.CLOSED;
        lock = new HashSet<>();
        createTime = Instant.now().toEpochMilli();
        updateTime = Instant.now().toEpochMilli();
    }

    public void add(BinlogActuator binlogActuator) {
        this.lock.add(binlogActuator);
    }

    public void remove(BinlogActuator binlogActuator) {
        this.lock.remove(binlogActuator);
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public BinlogStatusEnum getStatus() {
        return status;
    }

    public void setStatus(BinlogStatusEnum status) {
        this.status = status;
    }

    public Set<BinlogActuator> getLock() {
        return lock;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }
}
