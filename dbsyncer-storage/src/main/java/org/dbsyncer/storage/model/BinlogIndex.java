package org.dbsyncer.storage.model;

import org.dbsyncer.storage.binlog.BinlogActuator;
import org.dbsyncer.storage.enums.BinlogStatusEnum;

import java.time.LocalDateTime;
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
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    public BinlogIndex(String fileName, LocalDateTime createTime) {
        this.fileName = fileName;
        this.status = BinlogStatusEnum.CLOSED;
        this.lock = new HashSet<>();
        this.createTime = createTime;
        this.updateTime = LocalDateTime.now();
    }

    public void addLock(BinlogActuator binlogActuator) {
        synchronized (lock){
            this.lock.add(binlogActuator);
            this.status = BinlogStatusEnum.RUNNING;
        }
    }

    public void removeLock(BinlogActuator binlogActuator) {
        synchronized (lock) {
            this.lock.remove(binlogActuator);
            this.status = lock.isEmpty() ? BinlogStatusEnum.CLOSED : status;
        }
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

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(LocalDateTime updateTime) {
        this.updateTime = updateTime;
    }
}